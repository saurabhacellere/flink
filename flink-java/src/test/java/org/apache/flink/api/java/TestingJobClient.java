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

package org.apache.flink.api.java;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.OptionalFailure;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Testing implementation of {@link JobClient}.
 */
public class TestingJobClient implements JobClient {
	@Override
	public JobID getJobID() {
		return new JobID();
	}

	@Override
	public CompletableFuture<JobExecutionResult> getJobExecutionResult(ClassLoader userClassloader) {
		return CompletableFuture.completedFuture(new JobExecutionResult(new JobID(), 0L, Collections.emptyMap()));
	}

	@Override
	public CompletableFuture<JobStatus> getJobStatus() {
		return CompletableFuture.completedFuture(JobStatus.FINISHED);
	}

	@Override
	public CompletableFuture<Map<String, OptionalFailure<Object>>> getAccumulators(ClassLoader classLoader) {
		return CompletableFuture.completedFuture(Collections.emptyMap());
	}

	@Override
	public CompletableFuture<Acknowledge> cancel() {
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<String> stopWithSavepoint(boolean advanceToEndOfEventTime, @Nullable String savepointDirectory) {
		return CompletableFuture.completedFuture("null");
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(@Nullable String savepointDirectory) {
		return CompletableFuture.completedFuture("null");
	}

	@Override
	public void close() throws Exception {

	}
}
