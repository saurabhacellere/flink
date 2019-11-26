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

package org.apache.flink.streaming.api.scala

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.util.Collector
import org.junit.{Assert, Test}

import scala.collection.mutable.ListBuffer

class IntervalJoinITCase extends AbstractTestBase {

  @Test
  def testInclusiveBounds(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dataStream1 = env.fromElements(("key", 0L), ("key", 1L), ("key", 2L))
      .assignTimestampsAndWatermarks(new TimestampExtractor())
      .keyBy(elem => elem._1)

    val dataStream2 = env.fromElements(("key", 0L), ("key", 1L), ("key", 2L))
      .assignTimestampsAndWatermarks(new TimestampExtractor())
      .keyBy(elem => elem._1)

    val sink = new ResultSink()

    dataStream1.intervalJoin(dataStream2)
      .between(Time.milliseconds(0), Time.milliseconds(2))
      .process(new CombineToStringJoinFunction())
      .addSink(sink)

    env.execute()

    sink.expectInAnyOrder(
      "(key,0):(key,0)",
      "(key,0):(key,1)",
      "(key,0):(key,2)",

      "(key,1):(key,1)",
      "(key,1):(key,2)",

      "(key,2):(key,2)"
    )
  }

  @Test
  def testExclusiveBounds(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dataStream1 = env.fromElements(("key", 0L), ("key", 1L), ("key", 2L))
      .assignTimestampsAndWatermarks(new TimestampExtractor())
      .keyBy(elem => elem._1)

    val dataStream2 = env.fromElements(("key", 0L), ("key", 1L), ("key", 2L))
      .assignTimestampsAndWatermarks(new TimestampExtractor())
      .keyBy(elem => elem._1)

    val sink = new ResultSink()

    dataStream1.intervalJoin(dataStream2)
      .between(Time.milliseconds(0), Time.milliseconds(2))
      .lowerBoundExclusive()
      .upperBoundExclusive()
      .process(new CombineToStringJoinFunction())
      .addSink(sink)

    env.execute()

    sink.expectInAnyOrder(
      "(key,0):(key,1)",
      "(key,1):(key,2)"
    )
  }

  @Test
  @throws[Exception]
  def testUseLeftTimestamp(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val streamOne = env.fromElements(("key", 0L), ("key", 1L), ("key", 2L))
      .assignTimestampsAndWatermarks(new TimestampExtractor())
      .keyBy(elem => elem._1)

    val streamTwo = env.fromElements(("key", 0L), ("key", 1L), ("key", 2L))
      .assignTimestampsAndWatermarks(new TimestampExtractor())
      .keyBy(elem => elem._1)


    streamOne.intervalJoin(streamTwo)
      .between(Time.milliseconds(0L), Time.milliseconds(2L))
      .assignLeftTimestamp()
      .process(new ProcessJoinFunction[(String, Long), (String, Long), String]() {
        @throws[Exception]
        override def processElement(
            left: (String, Long),
            right: (String, Long),
            ctx: ProcessJoinFunction[(String, Long), (String, Long), String]#Context,
            out: Collector[String]): Unit = {

          Assert.assertEquals(ctx.getTimestamp, ctx.getLeftTimestamp)

        }
      })

    env.execute()
  }

  @Test
  @throws[Exception]
  def testUseRightTimestamp(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val streamOne = env.fromElements(("key", 1L), ("key", 2L), ("key", 3L))
      .assignTimestampsAndWatermarks(new TimestampExtractor())
      .keyBy(elem => elem._1)

    val streamTwo = env.fromElements(("key", 2L), ("key", 3L), ("key", 4L))
      .assignTimestampsAndWatermarks(new TimestampExtractor())
      .keyBy(elem => elem._1)

    streamOne.intervalJoin(streamTwo)
      .between(Time.milliseconds(-2L), Time.milliseconds(0L))
      .assignRightTimestamp()
      .process(new ProcessJoinFunction[(String, Long), (String, Long), String]() {
        @throws[Exception]
        override def processElement(
            left: (String, Long),
            right: (String, Long),
            ctx: ProcessJoinFunction[(String, Long), (String, Long), String]#Context,
            out: Collector[String]): Unit = {

          Assert.assertEquals(ctx.getTimestamp, ctx.getRightTimestamp)

        }
      })

    env.execute()
  }

  @Test
  @throws[Exception]
  def testUseMaxTimestamp(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val streamOne = env.fromElements(("key", 1L), ("key", 2L), ("key", 3L))
      .assignTimestampsAndWatermarks(new TimestampExtractor())
      .keyBy(elem => elem._1)

    val streamTwo = env.fromElements(("key", 2L), ("key", 3L), ("key", 4L))
      .assignTimestampsAndWatermarks(new TimestampExtractor())
      .keyBy(elem => elem._1)

    streamOne.intervalJoin(streamTwo)
      .between(Time.milliseconds(-2L), Time.milliseconds(0L))
      .assignMaxTimestamp()
      .process(new ProcessJoinFunction[(String, Long), (String, Long), String]() {
        @throws[Exception]
        override def processElement(
            left: (String, Long),
            right: (String, Long),
            ctx: ProcessJoinFunction[(String, Long), (String, Long), String]#Context,
            out: Collector[String]): Unit = {

          val expected = Math.max(ctx.getRightTimestamp, ctx.getLeftTimestamp)
          Assert.assertEquals(ctx.getTimestamp, expected)

        }
      })

    env.execute()
  }

  @Test
  @throws[Exception]
  def testUseMinTimestamp(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val streamOne = env.fromElements(("key", 0L), ("key", 1L), ("key", 2L))
      .assignTimestampsAndWatermarks(new TimestampExtractor())
      .keyBy(elem => elem._1)
    val streamTwo = env.fromElements(("key", 0L), ("key", 1L), ("key", 2L))
      .assignTimestampsAndWatermarks(new TimestampExtractor())
      .keyBy(elem => elem._1)

    streamOne.intervalJoin(streamTwo)
      .between(Time.milliseconds(-2L), Time.milliseconds(0L))
      .assignRightTimestamp()
      .process(new ProcessJoinFunction[(String, Long), (String, Long), String]() {
        @throws[Exception]
        override def processElement(
            left: (String, Long),
            right: (String, Long),
            ctx: ProcessJoinFunction[(String, Long), (String, Long), String]#Context,
            out: Collector[String]): Unit = {

          val expected = Math.min(ctx.getRightTimestamp, ctx.getLeftTimestamp)
          Assert.assertEquals(ctx.getTimestamp, expected)

        }
      })

    env.execute()
  }
}

object Companion {
  val results: ListBuffer[String] = new ListBuffer()
}

class ResultSink extends SinkFunction[String] {

  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    Companion.results.append(value)
  }

  def expectInAnyOrder(expected: String*): Unit = {
    Assert.assertTrue(expected.toSet.equals(Companion.results.toSet))
  }
}

class TimestampExtractor extends AscendingTimestampExtractor[(String, Long)] {
  override def extractAscendingTimestamp(element: (String, Long)): Long = element._2
}

class CombineToStringJoinFunction
  extends ProcessJoinFunction[(String, Long), (String, Long), String] {

  override def processElement(
                        left: (String, Long),
                        right: (String, Long),
                        ctx: ProcessJoinFunction[(String, Long), (String, Long), String]#Context,
                        out: Collector[String]): Unit = {
    out.collect(left + ":" + right)
  }
}
