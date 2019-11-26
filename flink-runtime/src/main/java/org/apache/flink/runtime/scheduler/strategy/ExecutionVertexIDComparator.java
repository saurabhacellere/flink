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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * This comparator compares {@link ExecutionVertexID} regarding the related job vertex topological index and
 * subtaskIndex. Smaller job vertex topological index indicates a smaller {@link ExecutionVertexID}. If the
 * job vertex topological index equals, smaller sub-task index indicates a smaller {@link ExecutionVertexID}.
 */
public class ExecutionVertexIDComparator implements Comparator<ExecutionVertexID> {

	final Map<JobVertexID, Integer> jobVertexTopologyIndices;

	public ExecutionVertexIDComparator(final List<JobVertexID> topologicallySortedJobVertexIDs) {
		jobVertexTopologyIndices = getJobVertexTopologyIndices(topologicallySortedJobVertexIDs);
	}

	private Map<JobVertexID, Integer> getJobVertexTopologyIndices(
			final List<JobVertexID> topologicallySortedJobVertexIDs) {

		return IntStream.range(0, topologicallySortedJobVertexIDs.size())
			.boxed()
			.collect(Collectors.toMap(
				index -> topologicallySortedJobVertexIDs.get(index),
				Function.identity()));
	}

	@Override
	public int compare(ExecutionVertexID id1, ExecutionVertexID id2) {
		final int jobVertexTopologyIndex1 = jobVertexTopologyIndices.getOrDefault(id1.getJobVertexId(), -1);
		final int jobVertexTopologyIndex2 = jobVertexTopologyIndices.getOrDefault(id2.getJobVertexId(), -1);

		checkState(jobVertexTopologyIndex1 >= 0);
		checkState(jobVertexTopologyIndex2 >= 0);

		if (jobVertexTopologyIndex1 > jobVertexTopologyIndex2) {
			return 1;
		} else if (jobVertexTopologyIndex1 < jobVertexTopologyIndex2) {
			return -1;
		} else {
			return id1.getSubtaskIndex() - id2.getSubtaskIndex();
		}
	}
}
