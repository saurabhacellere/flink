/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.api.java.operators.join.JoinType.FULL_OUTER;
import static org.apache.flink.api.java.operators.join.JoinType.INNER;
import static org.apache.flink.api.java.operators.join.JoinType.LEFT_OUTER;
import static org.apache.flink.api.java.operators.join.JoinType.RIGHT_OUTER;

/**
 * An {@link TwoInputStreamOperator operator} to execute time-bounded stream inner joins.
 *
 * <p>By using a configurable lower and upper bound this operator will emit exactly those pairs
 * (T1, T2) where t2.ts ∈ [T1.ts + lowerBound, T1.ts + upperBound]. Both the lower and the
 * upper bound can be configured to be either inclusive or exclusive.
 *
 * <p>As soon as elements are joined they are passed to a user-defined {@link ProcessJoinFunction}.
 *
 * <p>The basic idea of this implementation is as follows: Whenever we receive an element at
 * {@link #processElement1(StreamRecord)} (a.k.a. the left side), we add it to the left buffer.
 * We then check the right buffer to see whether there are any elements that can be joined. If
 * there are, they are joined and passed to the aforementioned function. The same happens the
 * other way around when receiving an element on the right side.
 *
 * <p>Whenever a pair of elements is emitted it will be assigned the max timestamp of either of
 * the elements.
 *
 * <p>In order to avoid the element buffers to grow indefinitely a cleanup timer is registered
 * per element. This timer indicates when an element is not considered for joining anymore and can
 * be removed from the state.
 *
 * @param <K>	The type of the key based on which we join elements.
 * @param <T1>	The type of the elements in the left stream.
 * @param <T2>	The type of the elements in the right stream.
 * @param <OUT>	The output type created by the user-defined function.
 */
@Internal
public class IntervalJoinOperator<K, T1, T2, OUT>
		extends AbstractUdfStreamOperator<OUT, ProcessJoinFunction<T1, T2, OUT>>
		implements TwoInputStreamOperator<T1, T2, OUT>, Triggerable<K, String> {

	private static final long serialVersionUID = -5380774605111543454L;

	private static final Logger logger = LoggerFactory.getLogger(IntervalJoinOperator.class);

	private static final String LEFT_BUFFER = "LEFT_BUFFER";
	private static final String RIGHT_BUFFER = "RIGHT_BUFFER";
	private static final String CLEANUP_TIMER_NAME = "CLEANUP_TIMER";
	private static final String CLEANUP_NAMESPACE_LEFT = "CLEANUP_LEFT";
	private static final String CLEANUP_NAMESPACE_RIGHT = "CLEANUP_RIGHT";

	private final long lowerBound;
	private final long upperBound;

	private final IntervalJoinOperator.TimestampStrategy timestampStrategy;
	private final JoinType joinType;

	private final TypeSerializer<T1> leftTypeSerializer;
	private final TypeSerializer<T2> rightTypeSerializer;

	private transient MapState<Long, List<BufferEntry<T1>>> leftBuffer;
	private transient MapState<Long, List<BufferEntry<T2>>> rightBuffer;

	private transient TimestampedCollector<OUT> collector;
	private transient ContextImpl context;

	private transient InternalTimerService<String> internalTimerService;

	/**
	 * Creates a new IntervalJoinOperator.
	 *
	 * @param lowerBound          The lower bound for evaluating if elements should be joined
	 * @param upperBound          The upper bound for evaluating if elements should be joined
	 * @param lowerBoundInclusive Whether or not to include elements where the timestamp matches
	 *                            the lower bound
	 * @param upperBoundInclusive Whether or not to include elements where the timestamp matches
	 *                            the upper bound
	 * @param udf                 A user-defined {@link ProcessJoinFunction} that gets called
	 *                            whenever two elements of T1 and T2 are joined
	 */
	public IntervalJoinOperator(
			long lowerBound,
			long upperBound,
			boolean lowerBoundInclusive,
			boolean upperBoundInclusive,
			TimestampStrategy timestampStrategy,
			JoinType joinType,
			TypeSerializer<T1> leftTypeSerializer,
			TypeSerializer<T2> rightTypeSerializer,
			ProcessJoinFunction<T1, T2, OUT> udf
	) {

		super(Preconditions.checkNotNull(udf));

		Preconditions.checkArgument(lowerBound <= upperBound,
			"lowerBound <= upperBound must be fulfilled");

		Preconditions.checkNotNull(timestampStrategy);

		// Move boundaries by +1 / -1 depending on inclusiveness in order not needing
		// to check for inclusiveness later on
		this.lowerBound = (lowerBoundInclusive) ? lowerBound : lowerBound + 1L;
		this.upperBound = (upperBoundInclusive) ? upperBound : upperBound - 1L;

		this.timestampStrategy = timestampStrategy;
		this.joinType = joinType;

		if (this.joinType == LEFT_OUTER && this.timestampStrategy == TimestampStrategy.RIGHT ||
			this.joinType == RIGHT_OUTER && this.timestampStrategy == TimestampStrategy.LEFT ||
			this.joinType == FULL_OUTER && this.timestampStrategy == TimestampStrategy.RIGHT ||
			this.joinType == FULL_OUTER && this.timestampStrategy == TimestampStrategy.LEFT) {

			throw new FlinkRuntimeException(
				"Cannot use JoinType." + joinType +
				" with TimestampStrategy." + timestampStrategy
			);
		}

		this.leftTypeSerializer = Preconditions.checkNotNull(leftTypeSerializer);
		this.rightTypeSerializer = Preconditions.checkNotNull(rightTypeSerializer);
	}

	@Override
	public void open() throws Exception {
		super.open();

		collector = new TimestampedCollector<>(output);
		context = new ContextImpl(userFunction);
		internalTimerService =
			getInternalTimerService(CLEANUP_TIMER_NAME, StringSerializer.INSTANCE, this);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

		this.leftBuffer = context.getKeyedStateStore().getMapState(new MapStateDescriptor<>(
			LEFT_BUFFER,
			LongSerializer.INSTANCE,
			new ListSerializer<>(new BufferEntrySerializer<>(leftTypeSerializer))
		));

		this.rightBuffer = context.getKeyedStateStore().getMapState(new MapStateDescriptor<>(
			RIGHT_BUFFER,
			LongSerializer.INSTANCE,
			new ListSerializer<>(new BufferEntrySerializer<>(rightTypeSerializer))
		));
	}

	/**
	 * Process a {@link StreamRecord} from the left stream. Whenever an {@link StreamRecord}
	 * arrives at the left stream, it will get added to the left buffer. Possible join candidates
	 * for that element will be looked up from the right buffer and if the pair lies within the
	 * user defined boundaries, it gets passed to the {@link ProcessJoinFunction}.
	 *
	 * @param record An incoming record to be joined
	 * @throws Exception Can throw an Exception during state access
	 */
	@Override
	public void processElement1(StreamRecord<T1> record) throws Exception {
		processElement(record, leftBuffer, rightBuffer, lowerBound, upperBound, true);
	}

	/**
	 * Process a {@link StreamRecord} from the right stream. Whenever a {@link StreamRecord}
	 * arrives at the right stream, it will get added to the right buffer. Possible join candidates
	 * for that element will be looked up from the left buffer and if the pair lies within the user
	 * defined boundaries, it gets passed to the {@link ProcessJoinFunction}.
	 *
	 * @param record An incoming record to be joined
	 * @throws Exception Can throw an exception during state access
	 */
	@Override
	public void processElement2(StreamRecord<T2> record) throws Exception {
		processElement(record, rightBuffer, leftBuffer, -upperBound, -lowerBound, false);
	}

	@SuppressWarnings("unchecked")
	private <THIS, OTHER> void processElement(
			final StreamRecord<THIS> record,
			final MapState<Long, List<IntervalJoinOperator.BufferEntry<THIS>>> ourBuffer,
			final MapState<Long, List<IntervalJoinOperator.BufferEntry<OTHER>>> otherBuffer,
			final long relativeLowerBound,
			final long relativeUpperBound,
			final boolean isLeft
	) throws Exception {

		final THIS ourValue = record.getValue();
		final long ourTimestamp = record.getTimestamp();

		if (ourTimestamp == Long.MIN_VALUE) {
			throw new FlinkException("Long.MIN_VALUE timestamp: Elements used in " +
					"interval stream joins need to have timestamps meaningful timestamps.");
		}

		if (isLate(ourTimestamp)) {
			return;
		}

		// we use this flag to indicate that the whole bucket needs to be written
		// back into the state in order to persist the information that an element
		// has been joined, which we need for outer joins in order to not emit
		// duplicates during cleanup
		boolean hasBeenJoined = false;

		for (Map.Entry<Long, List<BufferEntry<OTHER>>> bucket: otherBuffer.entries()) {

			final long timestamp  = bucket.getKey();
			final List<BufferEntry<OTHER>> otherEntries = bucket.getValue();

			if (timestamp < ourTimestamp + relativeLowerBound ||
					timestamp > ourTimestamp + relativeUpperBound) {
				continue;
			}

			for (BufferEntry<OTHER> entry : otherEntries) {
				if (isLeft) {
					collect((T1) ourValue, (T2) entry.element, ourTimestamp, timestamp);
					entry.hasBeenJoined = true;
					hasBeenJoined = true;
				} else {
					collect((T1) entry.element, (T2) ourValue, timestamp, ourTimestamp);
					entry.hasBeenJoined = true;
					hasBeenJoined = true;
				}
			}

			if (hasBeenJoined && joinType != INNER) {
				// write bucket back to state
				bucket.setValue(otherEntries);
			}
		}

		addToBuffer(ourBuffer, ourValue, ourTimestamp, hasBeenJoined);

		long cleanupTime = (relativeUpperBound > 0L) ? ourTimestamp + relativeUpperBound : ourTimestamp;
		if (isLeft) {
			internalTimerService.registerEventTimeTimer(CLEANUP_NAMESPACE_LEFT, cleanupTime);
		} else {
			internalTimerService.registerEventTimeTimer(CLEANUP_NAMESPACE_RIGHT, cleanupTime);
		}
	}

	private long calculateNecessaryWatermarkDelay() {
		switch (this.timestampStrategy) {
			case MIN:
				return Math.max(upperBound, -lowerBound);
			case MAX:
				return 0;
			case LEFT:
				return (upperBound < 0) ? 0 : upperBound;
			case RIGHT:
				return (lowerBound > 0) ? 0 : -lowerBound;
			default:
				throw new FlinkRuntimeException("Unsupported timestamp strategy: " + this.timestampStrategy);
		}
	}

	private boolean isLate(long timestamp) {
		long currentWatermark = internalTimerService.currentWatermark();
		return currentWatermark != Long.MIN_VALUE && timestamp < currentWatermark;
	}

	private void collect(T1 left, T2 right, Long leftTimestamp, Long rightTimestamp) throws Exception {
		final long resultTimestamp = calculateResultTimestamp(leftTimestamp, rightTimestamp);

		collector.setAbsoluteTimestamp(resultTimestamp);
		context.updateTimestamps(leftTimestamp, rightTimestamp, resultTimestamp);

		userFunction.processElement(left, right, context, collector);
	}

	private long calculateResultTimestamp(Long leftTs, Long rightTs) {

		switch (this.timestampStrategy) {
			case MIN:
				if (leftTs == null) {
					return rightTs;
				} else if (rightTs == null) {
					return leftTs;
				} else {
					return Math.min(leftTs, rightTs);
				}
			case MAX:
				if (leftTs == null) {
					return rightTs;
				} else if (rightTs == null) {
					return leftTs;
				} else {
					return Math.max(leftTs, rightTs);
				}
			case LEFT:
				return leftTs;
			case RIGHT:
				return rightTs;
			default:
				throw new FlinkRuntimeException("Unsupported timestamp strategy: " + timestampStrategy);
		}
	}

	private static <T> void addToBuffer(
			final MapState<Long, List<IntervalJoinOperator.BufferEntry<T>>> buffer,
			final T value,
			final long timestamp,
			boolean hasBeenJoined
	) throws Exception {

		List<BufferEntry<T>> elemsInBucket = buffer.get(timestamp);
		if (elemsInBucket == null) {
			elemsInBucket = new ArrayList<>();
		}
		elemsInBucket.add(new BufferEntry<>(value, hasBeenJoined));
		buffer.put(timestamp, elemsInBucket);
	}

	@Override
	public void onEventTime(InternalTimer<K, String> timer) throws Exception {

		long timerTimestamp = timer.getTimestamp();
		String namespace = timer.getNamespace();

		logger.trace("onEventTime @ {}", timerTimestamp);

		switch (namespace) {
			case CLEANUP_NAMESPACE_LEFT: {
				long timestamp = (upperBound <= 0L) ? timerTimestamp : timerTimestamp - upperBound;

				if (joinType == LEFT_OUTER || joinType == FULL_OUTER) {
					List<BufferEntry<T1>> leftElems = leftBuffer.get(timestamp);
					if (leftElems != null) {
						for (BufferEntry<T1> entry : leftElems) {
							if (!entry.hasBeenJoined) {
								collect(entry.element, null, timestamp, null);
							}
						}
					}
				}

				logger.trace("Removing from left buffer @ {}", timestamp);
				leftBuffer.remove(timestamp);

				break;
			}
			case CLEANUP_NAMESPACE_RIGHT: {
				long timestamp = (lowerBound <= 0L) ? timerTimestamp + lowerBound : timerTimestamp;

				if (joinType == RIGHT_OUTER || joinType == FULL_OUTER) {
					List<BufferEntry<T2>> rightElems = rightBuffer.get(timestamp);
					if (rightElems != null) {
						for (BufferEntry<T2> entry : rightElems) {
							if (!entry.hasBeenJoined) {
								collect(null, entry.element, null, timestamp);
							}
						}
					}
				}

				logger.trace("Removing from right buffer @ {}", timestamp);
				rightBuffer.remove(timestamp);
				break;
			}
			default:
				throw new RuntimeException("Invalid namespace " + namespace);
		}
	}

	@Override
	public void onProcessingTime(InternalTimer<K, String> timer) throws Exception {
		// do nothing.
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		if (timeServiceManager != null) {
			// Advance the timerService according to the "original" watermark.
			// This means that all timers that this operator registers as well as timers
			// registered via the UDF are with respect to the watermark as it comes in,
			// as opposed to the possibly delayed watermark emitted by the operator
			timeServiceManager.advanceWatermark(mark);
		}

		// Delay the watermark so that even in scenarios where the results of a join
		// are from elements that were buffered and watermark has progressed in the meantime,
		// we will never produce late data
		long delayedTimestamp = mark.getTimestamp() - calculateNecessaryWatermarkDelay();
		Watermark delayedWatermark = new Watermark(delayedTimestamp);

		output.emitWatermark(delayedWatermark);
	}

	/**
	 * The context that is available during an invocation of
	 * {@link ProcessJoinFunction#processElement(Object, Object, ProcessJoinFunction.Context, Collector)}.
	 *
	 * <p>It gives access to the timestamps of the left element in the joined pair, the right one, and that of
	 * the joined pair. In addition, this context allows to emit elements on a side output.
	 */
	private final class ContextImpl extends ProcessJoinFunction<T1, T2, OUT>.Context {

		private long resultTimestamp = Long.MIN_VALUE;

		private Long leftTimestamp = Long.MIN_VALUE;

		private Long rightTimestamp = Long.MIN_VALUE;

		private ContextImpl(ProcessJoinFunction<T1, T2, OUT> func) {
			func.super();
		}

		private void updateTimestamps(Long left, Long right, Long result) {
			this.leftTimestamp = left;
			this.rightTimestamp = right;
			this.resultTimestamp = result;
		}

		@Override
		public Long getLeftTimestamp() {
			return leftTimestamp;
		}

		@Override
		public Long getRightTimestamp() {
			return rightTimestamp;
		}

		@Override
		public Long getTimestamp() {
			return resultTimestamp;
		}

		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			Preconditions.checkArgument(outputTag != null, "OutputTag must not be null");
			output.collect(outputTag, new StreamRecord<>(value, getTimestamp()));
		}
	}

	/**
	 * TimestampStrategy defines which timestamp of the two joined elements to
	 * assign to the joined pair.
	 */
	public enum TimestampStrategy {

		/**
		 * When two elements are joined, assign the minimum timestamp of those two
		 * elements as the timestamp of the joined pair.
		 */
		MIN,

		/**
		 * When two elements are joined, assign the maximum timestamp of those two
		 * elements as the timestamp of the joined pair.
		 */
		MAX,

		/**
		 * When two elements are joined, assign the timestamp of the left element
		 * as the the timestamp of the joined pair.
		 */
		LEFT,

		/**
		 * When two elements are joined, assign the timestamp of the right element
		 * as the the timestamp of the joined pair.
		 */
		RIGHT
	}

	/**
	 * A container for elements put in the left/write buffer.
	 * This will contain the element itself along with a flag indicating
	 * if it has been joined or not.
	 */
	private static class BufferEntry<T> {

		private final T element;
		private boolean hasBeenJoined;

		BufferEntry(T element, boolean hasBeenJoined) {
			this.element = element;
			this.hasBeenJoined = hasBeenJoined;
		}
	}

	/**
	 * A {@link TypeSerializer serializer} for the {@link BufferEntry}.
	 */
	private static class BufferEntrySerializer<T> extends TypeSerializer<BufferEntry<T>> {

		private static final long serialVersionUID = -20197698803836236L;

		private final TypeSerializer<T> elementSerializer;

		private BufferEntrySerializer(TypeSerializer<T> elementSerializer) {
			this.elementSerializer = Preconditions.checkNotNull(elementSerializer);
		}

		@Override
		public boolean isImmutableType() {
			return true;
		}

		@Override
		public TypeSerializer<BufferEntry<T>> duplicate() {
			return new BufferEntrySerializer<>(elementSerializer.duplicate());
		}

		@Override
		public BufferEntry<T> createInstance() {
			return null;
		}

		@Override
		public BufferEntry<T> copy(BufferEntry<T> from) {
			return new BufferEntry<>(from.element, from.hasBeenJoined);
		}

		@Override
		public BufferEntry<T> copy(BufferEntry<T> from, BufferEntry<T> reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(BufferEntry<T> record, DataOutputView target) throws IOException {
			target.writeBoolean(record.hasBeenJoined);
			elementSerializer.serialize(record.element, target);
		}

		@Override
		public BufferEntry<T> deserialize(DataInputView source) throws IOException {
			boolean hasBeenJoined = source.readBoolean();
			T element = elementSerializer.deserialize(source);
			return new BufferEntry<>(element, hasBeenJoined);
		}

		@Override
		public BufferEntry<T> deserialize(BufferEntry<T> reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			target.writeBoolean(source.readBoolean());
			elementSerializer.copy(source, target);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			BufferEntrySerializer<?> that = (BufferEntrySerializer<?>) o;
			return Objects.equals(elementSerializer, that.elementSerializer);
		}

		@Override
		public int hashCode() {
			return Objects.hash(elementSerializer);
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj.getClass().equals(BufferEntrySerializer.class);
		}

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			return new BufferSerializerConfigSnapshot<>(elementSerializer);
		}

		@Override
		public CompatibilityResult<BufferEntry<T>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			if (configSnapshot instanceof BufferSerializerConfigSnapshot) {
				Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> previousSerializerAndConfig =
						((BufferSerializerConfigSnapshot) configSnapshot).getSingleNestedSerializerAndConfig();

				CompatibilityResult<T> compatResult =
						CompatibilityUtil.resolveCompatibilityResult(
								previousSerializerAndConfig.f0,
								UnloadableDummyTypeSerializer.class,
								previousSerializerAndConfig.f1,
								elementSerializer);

				if (!compatResult.isRequiresMigration()) {
					return CompatibilityResult.compatible();
				} else if (compatResult.getConvertDeserializer() != null) {
					return CompatibilityResult.requiresMigration(
							new BufferEntrySerializer<>(
									new TypeDeserializerAdapter<>(
											compatResult.getConvertDeserializer())));
				}
			}
			return CompatibilityResult.requiresMigration();
		}
	}

	/**
	 * The {@link CompositeTypeSerializerConfigSnapshot configuration} of our serializer.
	 */
	public static class BufferSerializerConfigSnapshot<T> extends CompositeTypeSerializerConfigSnapshot {

		private static final int VERSION = 1;

		public BufferSerializerConfigSnapshot() {
		}

		public BufferSerializerConfigSnapshot(final TypeSerializer<T> userTypeSerializer) {
			super(userTypeSerializer);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}
	}

	@VisibleForTesting
	MapState<Long, List<BufferEntry<T1>>> getLeftBuffer() {
		return leftBuffer;
	}

	@VisibleForTesting
	MapState<Long, List<BufferEntry<T2>>> getRightBuffer() {
		return rightBuffer;
	}

}
