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
package org.apache.flink.table.runtime.harness

import java.lang.{Boolean => JBool, Integer => JInt}
import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.datastream.{DataStream => JDataStream}
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.harness.HarnessTestBase._
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.sinks.{TableSink, UpsertStreamTableSink}
import org.apache.flink.table.utils.{Top2EmitUpdate, Top3WithMapView, Top3WithRetractInput}
import org.apache.flink.types.Row
import org.junit.Test

import scala.collection.mutable

class TableAggregateHarnessTest extends HarnessTestBase {

  protected var queryConfig =
    new TestStreamQueryConfig(Time.seconds(2), Time.seconds(3))
  val data = new mutable.MutableList[(Int, Int)]

  @Test
  def testTableAggregate(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    val top3 = new Top3WithMapView
    tEnv.registerFunction("top3", top3)
    val source = env.fromCollection(data).toTable(tEnv, 'a, 'b)
    val resultTable = source
      .groupBy('a)
      .flatAggregate(top3('b) as ('b1, 'b2))
      .select('a, 'b1, 'b2)

    val testHarness = createHarnessTester[Int, CRow, CRow](
      resultTable.toRetractStream[Row](queryConfig), "groupBy: (a)")

    testHarness.open()

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    // input with two columns: key and value
    testHarness.processElement(new StreamRecord(CRow(1: JInt, 1: JInt), 1))
    // output with three columns: key, value, value. The value is in the top3 of the key
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1: JInt, 1: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 1: JInt, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1: JInt, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2: JInt, 2: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 3: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 1: JInt, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 2: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1: JInt, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 3: JInt, 3: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 1: JInt, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 2: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 3: JInt, 3: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 3: JInt, 3: JInt), 1))

    // ingest data with key value of 2
    testHarness.processElement(new StreamRecord(CRow(2: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(2: JInt, 2: JInt, 2: JInt), 1))

    // trigger cleanup timer
    testHarness.setProcessingTime(3002)
    testHarness.processElement(new StreamRecord(CRow(1: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2: JInt, 2: JInt), 1))

    val result = testHarness.getOutput

    verify(expectedOutput, result)
    testHarness.close()
  }

  @Test
  def testEmitUpdateWithRetract(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    val top2 = new Top2EmitUpdate
    val source = env.fromCollection(data).toTable(tEnv, 'a, 'b)
    val resultTable = source
      .groupBy('a)
      .flatAggregate(top2('b) as ('b1, 'b2))
      .select('a, 'b1, 'b2)

    val testHarness = createHarnessTester[Int, CRow, CRow](
      resultTable.toRetractStream[Row](queryConfig), "groupBy: (a)")

    testHarness.open()

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    // input with two columns: key and value
    testHarness.processElement(new StreamRecord(CRow(1: JInt, 1: JInt), 1))
    // output with three columns: key, value, value. The value is in the top3 of the key
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1: JInt, 1: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 1: JInt, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2: JInt, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1: JInt, 2: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 4: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 2: JInt, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 4: JInt, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 1: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2: JInt, 2: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 3: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 2: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 3: JInt, 2: JInt), 1))

    // trigger cleanup timer
    testHarness.setProcessingTime(3002)
    testHarness.processElement(new StreamRecord(CRow(1: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2: JInt, 1: JInt), 1))

    val result = testHarness.getOutput

    verify(expectedOutput, result)
    testHarness.close()
  }

  @Test
  def testEmitUpdateWithoutRetract(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    val upsertSink = new ExtractDataStreamUpsertSink().configure(
      Array[String]("a", "v", "rank"),
      Array[TypeInformation[_]](Types.INT, Types.INT, Types.INT))

    tEnv.registerTableSink("upsertSink", upsertSink)

    val top2 = new Top2EmitUpdate
    val source = env.fromCollection(data).toTable(tEnv, 'a, 'b)
    source
      .groupBy('a)
      .flatAggregate(top2('b) as ('v, 'rank) withKeys 'rank)
      .select('a, 'v, 'rank).insertInto(queryConfig, "upsertSink")

    val testHarness = createHarnessTester[Int, CRow, CRow](
      upsertSink.asInstanceOf[ExtractDataStreamUpsertSink]
        .getDataStream, "groupBy: (a)")

    testHarness.open()

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    // input with two columns: key and value
    testHarness.processElement(new StreamRecord(CRow(1: JInt, 1: JInt), 1))
    // output with three columns: key, value, value. The value is in the top3 of the key
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1: JInt, 1: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2: JInt, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1: JInt, 2: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 4: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 4: JInt, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2: JInt, 2: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(1: JInt, 3: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 3: JInt, 2: JInt), 1))

    // trigger cleanup timer
    testHarness.setProcessingTime(3002)
    testHarness.processElement(new StreamRecord(CRow(1: JInt, 2: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 2: JInt, 1: JInt), 1))

    val result = testHarness.getOutput

    verify(expectedOutput, result)
    testHarness.close()
  }

  @Test
  def testTableAggregateWithRetractInput(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    val top3 = new Top3WithRetractInput
    tEnv.registerFunction("top3", top3)
    val source = env.fromCollection(data).toTable(tEnv, 'a, 'b)
    val resultTable = source
      .groupBy('a)
      .select('b.sum as 'b)
      .flatAggregate(top3('b) as ('b1, 'b2))
      .select('b1, 'b2)

    val testHarness = createHarnessTester[Int, CRow, CRow](
      resultTable.toRetractStream[Row](queryConfig), "select: (Top3WithRetractInput")

    testHarness.open()

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    // input with two columns: key and value
    testHarness.processElement(new StreamRecord(CRow(1: JInt), 1))
    // output with three columns: key, value, value. The value is in the top3 of the key
    expectedOutput.add(new StreamRecord(CRow(1: JInt, 1: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(false, 1: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 1: JInt, 1: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(3: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(3: JInt, 3: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(4: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 3: JInt, 3: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(3: JInt, 3: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(4: JInt, 4: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(false, 3: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 3: JInt, 3: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 4: JInt, 4: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(4: JInt, 4: JInt), 1))

    testHarness.processElement(new StreamRecord(CRow(5: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(false, 4: JInt, 4: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(4: JInt, 4: JInt), 1))
    expectedOutput.add(new StreamRecord(CRow(5: JInt, 5: JInt), 1))

    val result = testHarness.getOutput

    verify(expectedOutput, result)
    testHarness.close()
  }
}

/**
  * Use this upsert sink to get the emitted DataStream. The DataStream can be used to
  * create the harness tester.
  */
private[flink] class ExtractDataStreamUpsertSink extends UpsertStreamTableSink[Row] {

  var fNames: Array[String] = _
  var fTypes: Array[TypeInformation[_]] = _
  var dataStream: DataStream[JTuple2[JBool, Row]] = _

  def getDataStream: DataStream[JTuple2[JBool, Row]] = dataStream

  override def setKeyFields(keys: Array[String]): Unit = {}

  override def setIsAppendOnly(isAppendOnly: JBool): Unit = {}

  override def getRecordType: TypeInformation[Row] = new RowTypeInfo(fTypes, fNames)

  override def emitDataStream(s: JDataStream[JTuple2[JBool, Row]]): Unit = {
    dataStream = new DataStream(s)
  }

  override def getFieldNames: Array[String] = fNames

  override def getFieldTypes: Array[TypeInformation[_]] = fTypes

  override def configure(
    fieldNames: Array[String],
    fieldTypes: Array[TypeInformation[_]]): TableSink[JTuple2[JBool, Row]] = {
    val copy = new ExtractDataStreamUpsertSink
    copy.fNames = fieldNames
    copy.fTypes = fieldTypes
    copy
  }
}
