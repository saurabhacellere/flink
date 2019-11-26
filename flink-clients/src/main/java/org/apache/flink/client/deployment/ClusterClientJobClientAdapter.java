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

package org.apache.flink.client.deployment;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link JobClient} interface that uses a {@link ClusterClient} underneath..
 */
public class ClusterClientJobClientAdapter<ClusterID> implements JobClient {

	private static final Logger LOG = LoggerFactory.getLogger(ClusterClientJobClientAdapter.class);

	private final ClusterClient<ClusterID> clusterClient;

	private final JobID jobID;

	private final List<ThrowingRunnable<?>> onCloseActions;

	public ClusterClientJobClientAdapter(
			final ClusterClient<ClusterID> clusterClient,
			final JobID jobID) {
		this.jobID = checkNotNull(jobID);
		this.clusterClient = checkNotNull(clusterClient);
		this.onCloseActions = new ArrayList<>();
	}

	@Override
	public JobID getJobID() {
		return jobID;
	}

	@Override
	public CompletableFuture<JobStatus> getJobStatus() {
		return clusterClient.getJobStatus(jobID);
	}

	@Override
	public CompletableFuture<Map<String, OptionalFailure<Object>>> getAccumulators(ClassLoader classLoader) {
		return clusterClient.getAccumulators(jobID, classLoader);
	}

	@Override
	public CompletableFuture<Acknowledge> cancel() {
		return clusterClient.cancel(jobID);
	}

	@Override
	public CompletableFuture<String> stopWithSavepoint(boolean advanceToEndOfEventTime, @Nullable String savepointDirectory) {
		return clusterClient.stopWithSavepoint(jobID, advanceToEndOfEventTime, savepointDirectory);
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(@Nullable String savepointDirectory) {
		return clusterClient.triggerSavepoint(jobID, savepointDirectory);
	}

	@Override
	public CompletableFuture<JobExecutionResult> getJobExecutionResult(final ClassLoader userClassloader) {
		checkNotNull(userClassloader);

		final CompletableFuture<JobResult> jobResultFuture = clusterClient.requestJobResult(jobID);
		return jobResultFuture.handle((jobResult, throwable) -> {
			if (throwable != null) {
				ExceptionUtils.checkInterrupted(throwable);
				throw new CompletionException(new ProgramInvocationException("Could not run job", jobID, throwable));
			} else {
				try {
					return jobResult.toJobExecutionResult(userClassloader);
				} catch (JobExecutionException | IOException | ClassNotFoundException e) {
					throw new CompletionException(new ProgramInvocationException("Job failed", jobID, e));
				}
			}
		});
	}

	void addOnCloseActions(Collection<ThrowingRunnable<?>> action) {
		onCloseActions.addAll(action);
	}

	@Override
	public void close() throws Exception {
		Throwable throwable = null;

		for (ThrowingRunnable<?> action : onCloseActions) {
			try {
				action.run();
			} catch (Throwable t) {
				throwable = ExceptionUtils.firstOrSuppressed(t, throwable);
			}
		}

		if (throwable != null) {
			throw new Exception("Cannot close JobClient properly.", throwable);
		}
	}
}
