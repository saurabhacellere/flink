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
import org.apache.flink.formats.compressfile.extractor.DefaultExtractor;
import org.apache.flink.formats.compressfile.extractor.Extractor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

/**
 * A builder that creates a CompressFileWriter {@link CompressFileWriter}.
 *
 * @param <IN> The type element to write.
 */
@PublicEvolving
public class CompressFileWriterBuilder<IN> {
	private Extractor<IN> extractor = new DefaultExtractor<>();
	private CompressionCodec hadoopCodec;

	public CompressFileWriterBuilder<IN> setExtractor(Extractor<IN> extractor) {
		this.extractor = extractor;
		return this;
	}

	public CompressFileWriterBuilder<IN> setHadoopCodec(CompressionCodec hadoopCodec) {
		this.hadoopCodec = hadoopCodec;
		return this;
	}

	public CompressFileWriterBuilder<IN> setHadoopCodec(String hadoopCodecName) {
		return setHadoopCodec(hadoopCodecName, new Configuration());
	}

	public CompressFileWriterBuilder<IN> setHadoopCodec(String hadoopCodecName, Configuration hadoopConfiguration) {
		this.hadoopCodec = new CompressionCodecFactory(hadoopConfiguration).getCodecByName(hadoopCodecName);
		return this;
	}

	public CompressFileWriter<IN> build() {
		return new CompressFileWriter<>(extractor, hadoopCodec);
	}
}
