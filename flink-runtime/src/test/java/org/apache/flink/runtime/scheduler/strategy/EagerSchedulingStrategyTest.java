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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.scheduler.strategy.StrategyTestUtil.getExecutionVertexIdsFromDeployOptions;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link EagerSchedulingStrategy}.
 */
public class EagerSchedulingStrategyTest extends TestLogger {

	private TestingSchedulerOperations testingSchedulerOperations;

	private TestingSchedulingTopology testingSchedulingTopology;

	private EagerSchedulingStrategy schedulingStrategy;

	@Before
	public void setUp() {
		testingSchedulerOperations = new TestingSchedulerOperations();
		testingSchedulingTopology = new TestingSchedulingTopology();
		schedulingStrategy = new EagerSchedulingStrategy(
				testingSchedulerOperations,
				testingSchedulingTopology);
	}

	/**
	 * Tests that when start scheduling eager scheduling strategy will start all vertices in scheduling topology.
	 */
	@Test
	public void testStartScheduling() {
		final JobVertexID jobVertexID = new JobVertexID();
		final List<TestingSchedulingExecutionVertex> executionVertices = Arrays.asList(
			new TestingSchedulingExecutionVertex(jobVertexID, 4),
			new TestingSchedulingExecutionVertex(jobVertexID, 0),
			new TestingSchedulingExecutionVertex(jobVertexID, 2),
			new TestingSchedulingExecutionVertex(jobVertexID, 1),
			new TestingSchedulingExecutionVertex(jobVertexID, 3));
		testingSchedulingTopology.addSchedulingExecutionVertices(executionVertices);

		schedulingStrategy.startScheduling();

		assertThat(testingSchedulerOperations.getScheduledVertices(), hasSize(1));

		final Collection<ExecutionVertexDeploymentOption> scheduledVertices = testingSchedulerOperations.getScheduledVertices().get(0);
		assertThat(scheduledVertices, instanceOf(List.class));

		final List<ExecutionVertexID> scheduledVertexIDs = getExecutionVertexIdsFromDeployOptions((List) scheduledVertices);

		final List<ExecutionVertexID> executionVertexIDs = executionVertices.stream()
			.map(TestingSchedulingExecutionVertex::getId)
			.collect(Collectors.toList());
		assertEquals(executionVertexIDs, scheduledVertexIDs);
	}

	/**
	 * Tests that eager scheduling strategy will restart all vertices needing restarted at same time.
	 */
	@Test
	public void testRestartTasks() {
		final JobVertexID jobVertexID = new JobVertexID();
		final List<TestingSchedulingExecutionVertex> executionVertices = Arrays.asList(
			new TestingSchedulingExecutionVertex(jobVertexID, 0),
			new TestingSchedulingExecutionVertex(jobVertexID, 1),
			new TestingSchedulingExecutionVertex(jobVertexID, 2),
			new TestingSchedulingExecutionVertex(jobVertexID, 3),
			new TestingSchedulingExecutionVertex(jobVertexID, 4));
		testingSchedulingTopology.addSchedulingExecutionVertices(executionVertices);

		final List<ExecutionVertexID> verticesToRestart1 = Arrays.asList(
			new ExecutionVertexID(jobVertexID, 0),
			new ExecutionVertexID(jobVertexID, 4));
		schedulingStrategy.restartTasks(verticesToRestart1);

		final List<ExecutionVertexID> verticesToRestart2 = Arrays.asList(
			new ExecutionVertexID(jobVertexID, 2),
			new ExecutionVertexID(jobVertexID, 1),
			new ExecutionVertexID(jobVertexID, 3));
		schedulingStrategy.restartTasks(verticesToRestart2);

		assertThat(testingSchedulerOperations.getScheduledVertices(), hasSize(2));

		final Collection<ExecutionVertexDeploymentOption> scheduledVertices1 =
			testingSchedulerOperations.getScheduledVertices().get(0);
		assertThat(scheduledVertices1, instanceOf(List.class));
		assertEquals(verticesToRestart1, getExecutionVertexIdsFromDeployOptions((List) scheduledVertices1));

		final Collection<ExecutionVertexDeploymentOption> scheduledVertices2 =
			testingSchedulerOperations.getScheduledVertices().get(1);
		assertThat(scheduledVertices1, instanceOf(List.class));
		assertEquals(verticesToRestart2, getExecutionVertexIdsFromDeployOptions((List) scheduledVertices2));
	}
}
