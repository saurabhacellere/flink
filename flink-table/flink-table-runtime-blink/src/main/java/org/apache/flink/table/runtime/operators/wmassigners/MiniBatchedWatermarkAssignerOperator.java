/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.wmassigners;

import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.generated.WatermarkGenerator;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A stream operator that extracts timestamps from stream elements and
 * generates watermarks with specified emit latency.
 *
 * <p>The difference between this operator and {@link WatermarkAssignerOperator} is that:
 * <ul>
 *     <li>This operator has an additional parameter {@link #minibatchInterval} which is
 *     used to buffer watermarks and emit in an aligned interval with following window operators.</li>
 * </ul>
 */
public class MiniBatchedWatermarkAssignerOperator
	extends AbstractStreamOperator<BaseRow>
	implements OneInputStreamOperator<BaseRow, BaseRow>, ProcessingTimeCallback {

	private static final long serialVersionUID = 1L;

	/** The field index of rowtime attribute. */
	private final int rowtimeFieldIndex;

	/** The watermark generator which generates watermark from the input row. */
	private final WatermarkGenerator watermarkGenerator;

	/** The idle timeout for how long it doesn't receive elements to mark this channel idle. */
	private final long idleTimeout;

	/** The event-time interval for emitting watermarks. */
	private final long minibatchInterval;

	/** Current watermark of this operator, but may not be emitted. */
	private transient long currentWatermark;

	/** The next watermark to be emitted. */
	private transient long nextWatermark;

	/** The processing time when the last record is processed. */
	private transient long lastRecordTime;

	/** Channel status maintainer which is used to inactive channel when channel is idle. */
	private transient StreamStatusMaintainer streamStatusMaintainer;

	/** Flag to prevent duplicate function.close() calls in close() and dispose(). */
	private transient boolean functionsClosed = false;

	public MiniBatchedWatermarkAssignerOperator(
			int rowtimeFieldIndex,
			WatermarkGenerator watermarkGenerator,
			long idleTimeout,
			long minibatchInterval) {
		checkArgument(minibatchInterval > 0, "The inferred emit latency should be larger than 0");
		this.rowtimeFieldIndex = rowtimeFieldIndex;
		this.watermarkGenerator = watermarkGenerator;
		this.idleTimeout = idleTimeout;
		this.minibatchInterval = minibatchInterval;
		this.chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();

		currentWatermark = 0;
		nextWatermark = getMiniBatchStart(currentWatermark, minibatchInterval) + minibatchInterval - 1;

		if (idleTimeout > 0) {
			this.lastRecordTime = getProcessingTimeService().getCurrentProcessingTime();
			this.streamStatusMaintainer = getContainingTask().getStreamStatusMaintainer();
			getProcessingTimeService().registerTimer(lastRecordTime + idleTimeout, this);
		}

		FunctionUtils.setFunctionRuntimeContext(watermarkGenerator, getRuntimeContext());
		FunctionUtils.openFunction(watermarkGenerator, new Configuration());
	}

	@Override
	public void processElement(StreamRecord<BaseRow> element) throws Exception {
		if (idleTimeout > 0) {
			// mark the channel active
			streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
			lastRecordTime = getProcessingTimeService().getCurrentProcessingTime();
		}
		BaseRow row = element.getValue();
		if (row.isNullAt(rowtimeFieldIndex)) {
			throw new RuntimeException("RowTime field should not be null," +
				" please convert it to a non-null long value.");
		}
		Long watermark = watermarkGenerator.currentWatermark(row);
		if (watermark != null) {
			currentWatermark = Math.max(currentWatermark, watermark);
		}
		// forward element
		output.collect(element);

		// emit watermark if reach to the next watermark
		if (currentWatermark >= nextWatermark) {
			advanceWatermark();
		}
	}

	private void advanceWatermark() {
		output.emitWatermark(new Watermark(currentWatermark));
		long start = getMiniBatchStart(currentWatermark, minibatchInterval);
		long end = start + minibatchInterval - 1;
		nextWatermark = end > currentWatermark ? end : end + minibatchInterval;
	}

	/**
	 * The processing time trigger is only used for idle timeout. This will not flush watermarks
	 * because watermark advancing is all controlled by {@link #processElement(StreamRecord)}.
	 */
	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		if (idleTimeout > 0) {
			final long currentTime = getProcessingTimeService().getCurrentProcessingTime();
			if (currentTime - lastRecordTime > idleTimeout) {
				// mark the channel as idle to ignore watermarks from this channel
				streamStatusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
			}
		}

		// register next timer
		long now = getProcessingTimeService().getCurrentProcessingTime();
		getProcessingTimeService().registerTimer(now + idleTimeout, this);
	}

	/**
	 * Override the base implementation to completely ignore watermarks propagated from
	 * upstream (we rely only on the {@link MiniBatchedWatermarkAssignerOperator} to emit
	 * watermarks from here).
	 */
	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// if we receive a Long.MAX_VALUE watermark we forward it since it is used
		// to signal the end of input and to not block watermark progress downstream
		if (mark.getTimestamp() == Long.MAX_VALUE && currentWatermark != Long.MAX_VALUE) {
			if (idleTimeout > 0) {
				// mark the channel active
				streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
			}
			currentWatermark = Long.MAX_VALUE;
			output.emitWatermark(mark);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();

		// emit a final watermark
		advanceWatermark();

		functionsClosed = true;
		FunctionUtils.closeFunction(watermarkGenerator);
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		if (!functionsClosed) {
			functionsClosed = true;
			FunctionUtils.closeFunction(watermarkGenerator);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------
	/**
	 * Method to get the mini-batch start for a watermark.
	 */
	private static long getMiniBatchStart(long watermark, long interval) {
		return watermark - (watermark + interval) % interval;
	}
}
