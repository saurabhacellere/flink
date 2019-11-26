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
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link ExecutionVertexIDComparator}.
 */
public class ExecutionVertexIDComparatorTest extends TestLogger {

	private final JobVertexID vid1 = new JobVertexID();

	private final JobVertexID vid2 = new JobVertexID();

	private final List<JobVertexID> topologicallySortedJobVertexIDs = Arrays.asList(vid1, vid2);

	private final ExecutionVertexIDComparator executionVertexIDComparator =
		new ExecutionVertexIDComparator(topologicallySortedJobVertexIDs);

	@Test
	public void testCompareIDsFromDifferentJobVertex() {
		assertThat(
			executionVertexIDComparator.compare(new ExecutionVertexID(vid1, 0), new ExecutionVertexID(vid2, 0)),
			lessThan(0));
	}

	@Test
	public void testCompareIDsWithDifferentSubTaskIndexFromSameJobVertex() {
		assertThat(
			executionVertexIDComparator.compare(new ExecutionVertexID(vid1, 2), new ExecutionVertexID(vid1, 0)),
			greaterThan(0));
	}

	@Test
	public void testCompareIDsWithDifferentSubTaskIndexFromDifferentJobVertex() {
		assertThat(
			executionVertexIDComparator.compare(new ExecutionVertexID(vid1, 2), new ExecutionVertexID(vid2, 0)),
			lessThan(0));
	}

	@Test
	public void testCompareIDsEqual() {
		assertThat(
			executionVertexIDComparator.compare(new ExecutionVertexID(vid1, 2), new ExecutionVertexID(vid1, 2)),
			equalTo(0));
	}

	@Test(expected = IllegalStateException.class)
	public void testCompareIDsFromUnknownJobVertex() {
		executionVertexIDComparator.compare(
			new ExecutionVertexID(vid1, 0),
			new ExecutionVertexID(new JobVertexID(), 0));
	}
}
