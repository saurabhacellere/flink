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

package org.apache.flink.formats.compressfile;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.formats.compressfile.compressor.Compressor;
import org.apache.flink.formats.compressfile.compressor.HadoopCompressor;
import org.apache.flink.formats.compressfile.compressor.NoCompressor;
import org.apache.flink.formats.compressfile.extractor.Extractor;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;

/**
 * A {@link BulkWriter} implementation that compress file.
 *
 * @param <IN> The type of element to write.
 */
@PublicEvolving
public class CompressFileWriter<IN> implements BulkWriter.Factory<IN> {

	private Extractor<IN> extractor;
	private CompressionCodec hadoopCodec;

	CompressFileWriter(Extractor<IN> extractor, CompressionCodec hadoopCodec) {
		this.extractor = Preconditions.checkNotNull(extractor, "extractor cannot be null");
		this.hadoopCodec = hadoopCodec;
	}

	@Override
	public BulkWriter<IN> create(FSDataOutputStream out) throws IOException {
		try {
			Compressor<IN> compressor;
			if (hadoopCodec != null) {
				compressor = new HadoopCompressor<>(out, extractor, hadoopCodec);
			} else {
				compressor = new NoCompressor<>(out, extractor);
			}
			return compressor;
		} catch (Exception e) {
			throw new IOException(e.getLocalizedMessage(), e);
		}
	}

}
