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

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.core.execution.Executor;
import org.apache.flink.core.execution.ExecutorFactory;
import org.apache.flink.core.execution.ExecutorServiceLoader;
import org.apache.flink.core.execution.JobClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * The helper class to deploy a table program on the cluster.
 */
public class ProgramDeployer<C> {
	private static final Logger LOG = LoggerFactory.getLogger(ProgramDeployer.class);

	private final ExecutionContext<C> context;
	private final Pipeline pipeline;
	private final String jobName;
	private final boolean awaitJobResult;

	/**
	 * Deploys a table program on the cluster.
	 *
	 * @param context        context with deployment information
	 * @param jobName        job name of the Flink job to be submitted
	 * @param pipeline       Flink {@link Pipeline} to execute
	 * @param awaitJobResult block for a job execution result from the cluster
	 */
	public ProgramDeployer(
			ExecutionContext<C> context,
			String jobName,
			Pipeline pipeline,
			boolean awaitJobResult) {
		this.context = context;
		this.pipeline = pipeline;
		this.jobName = jobName;
		this.awaitJobResult = awaitJobResult;
	}

	public CompletableFuture<JobClient> run() {
		LOG.info("Submitting job {} for query {}`", pipeline, jobName);
		if (LOG.isDebugEnabled()) {
			LOG.debug("Submitting job {} with the following environment: \n{}",
					pipeline, context.getMergedEnvironment());
		}

		// create a copy so that we can change settings without affecting the original config
		Configuration configuration = new Configuration(context.getFlinkConfig());
		if (configuration.get(DeploymentOptions.TARGET) == null) {
			throw new RuntimeException("No execution.target specified in your configuration file.");
		}

		if (awaitJobResult) {
			configuration.set(DeploymentOptions.ATTACHED, true);
		}

		ExecutorServiceLoader executorServiceLoader = DefaultExecutorServiceLoader.INSTANCE;
		final ExecutorFactory executorFactory;
		try {
			executorFactory = executorServiceLoader.getExecutorFactory(configuration);
		} catch (Exception e) {
			throw new RuntimeException("Could not retrieve ExecutorFactory.", e);
		}

		final Executor executor = executorFactory.getExecutor(configuration);
		CompletableFuture<JobClient> jobClient;
		try {
			jobClient = executor.execute(pipeline, configuration);
		} catch (Exception e) {
			throw new RuntimeException("Could not execute program.", e);
		}
		return jobClient;
	}
}

