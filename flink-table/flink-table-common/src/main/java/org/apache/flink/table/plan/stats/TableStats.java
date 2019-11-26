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

package org.apache.flink.table.plan.stats;

import org.apache.flink.annotation.PublicEvolving;

import java.util.HashMap;
import java.util.Map;

/**
 * Table statistics.
 */
@PublicEvolving
public final class TableStats {
	public static final TableStats UNKNOWN = new TableStats(-1, new HashMap<>());

	/**
	 * cardinality of table.
	 */
	private final long rowCount;

	/**
	 * colStats statistics of table columns.
	 */
	private final Map<String, ColumnStats> colStats;

	public TableStats(long rowCount) {
		this(rowCount, new HashMap<>());
	}

	public TableStats(long rowCount, Map<String, ColumnStats> colStats) {
		this.rowCount = rowCount;
		this.colStats = colStats;
	}

	public long getRowCount() {
		return rowCount;
	}

	public Map<String, ColumnStats> getColumnStats() {
		return colStats;
	}

	/**
	 * Create a deep copy of "this" instance.
	 * @return a deep copy
	 */
	public TableStats copy() {
		TableStats copy = new TableStats(this.rowCount);
		for (Map.Entry<String, ColumnStats> entry : this.colStats.entrySet()) {
			copy.colStats.put(entry.getKey(), entry.getValue().copy());
		}
		return copy;
	}

	public TableStats merge(TableStats other) {
		Map<String, ColumnStats> colStats = new HashMap<>();
		HashMap<String, ColumnStats> otherColStats = new HashMap<>(other.colStats);
		for (Map.Entry<String, ColumnStats> entry : this.colStats.entrySet()) {
			String col = entry.getKey();
			ColumnStats stats = entry.getValue();
			ColumnStats otherStats = otherColStats.remove(col);
			stats = otherStats == null ? stats : stats.merge(otherStats);
			colStats.put(col, stats);
		}
		colStats.putAll(otherColStats);
		return new TableStats(this.rowCount + other.rowCount, colStats);
	}
}
