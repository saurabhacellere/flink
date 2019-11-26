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

package org.apache.flink.client.cli;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.commons.cli.CommandLine;

/**
 * Base test class for {@link CliFrontend} tests.
 */
public abstract class CliFrontendTestBase extends TestLogger {

	protected Configuration getConfiguration() {
		final Configuration configuration = GlobalConfiguration
			.loadConfiguration(CliFrontendTestUtils.getConfigDir());
		return configuration;
	}

	static AbstractCustomCommandLine<?> getCli(Configuration configuration) {
		return new DefaultCLI(configuration);
	}

	static int parseParametersAndRun(CliFrontend cliFrontend, String[] args) throws Exception {
		Tuple2<CommandLine, FunctionWithException<CommandLine, Integer, Exception>> commandLineAction = cliFrontend.parseParameters(args);
		return commandLineAction.f1.apply(commandLineAction.f0);
	}
}
