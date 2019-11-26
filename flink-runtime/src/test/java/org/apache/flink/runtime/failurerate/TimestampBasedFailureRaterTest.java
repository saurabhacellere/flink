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

package org.apache.flink.runtime.failurerate;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.util.clock.ManualClock;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
* Test time stamp based failure rater.
*/
public class TimestampBasedFailureRaterTest {

	@Test
	public void testMaximumFailureCheck() {
		TimestampBasedFailureRater rater = new TimestampBasedFailureRater(5, Time.of(10, TimeUnit.SECONDS));

		for (int i = 0; i < 6; i++) {
			rater.markEvent();
		}

		Assert.assertEquals(6.0, rater.getCurrentFailureRate(), 0.01);
		Assert.assertTrue(rater.exceedsFailureRate());
	}

	@Test
	public void testMovingRate() throws InterruptedException {
		FailureRater rater = new TimestampBasedFailureRater(5, Time.of(500, TimeUnit.MILLISECONDS));

		ManualClock manualClock = new ManualClock();
		manualClock.advanceTime(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
		for (int i = 0; i < 6; i++) {
			rater.markFailure(manualClock);
			manualClock.advanceTime(150, TimeUnit.MILLISECONDS);
			Thread.sleep(150);
		}

		Assert.assertEquals(3.0, rater.getCurrentFailureRate(), 0.01);
		Assert.assertFalse(rater.exceedsFailureRate());
	}
}
