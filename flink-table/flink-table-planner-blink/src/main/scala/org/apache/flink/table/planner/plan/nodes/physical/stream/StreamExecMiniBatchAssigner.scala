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

package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkTypeFactory}
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.`trait`.{MiniBatchIntervalTraitDef, MiniBatchMode}
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.utils.TableConfigUtils.getMillisecondFromConfigDuration
import org.apache.flink.table.runtime.operators.wmassigners.MiniBatchAssignerOperator
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo
import org.apache.flink.util.Preconditions

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for injecting a mini-batch event in the streaming data. The mini-batch
  * event will be recognized as a boundary between two mini-batches. The following operators will
  * keep track of the mini-batch events and trigger mini-batch once the mini-batch id is advanced.
  *
  * NOTE: currently, we leverage the runtime watermark mechanism to achieve the mini-batch, because
  * runtime doesn't support customized events and the watermark mechanism fully meets mini-batch
  * needs.
  */
class StreamExecMiniBatchAssigner(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    inputRel: RelNode)
  extends SingleRel(cluster, traits, inputRel)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow] {

  override def producesUpdates: Boolean = false

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = false

  override def consumesRetractions: Boolean = false

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecMiniBatchAssigner(
      cluster,
      traitSet,
      inputs.get(0))
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val miniBatchInterval = traits.getTrait(MiniBatchIntervalTraitDef.INSTANCE).getMiniBatchInterval
    val interval = if (miniBatchInterval.mode == MiniBatchMode.ProcTime) {
      val tableConfig = cluster.getPlanner.getContext.asInstanceOf[FlinkContext].getTableConfig
      val miniBatchLatency = getMillisecondFromConfigDuration(
        tableConfig,
        ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY)
      Preconditions.checkArgument(miniBatchLatency > 0,
        "MiniBatch latency must be greater that 0 ms.", null)
      s"${miniBatchLatency}ms"
    } else {
      throw new TableException(s"MiniBatchAssigner shouldn't be in ${miniBatchInterval.mode} " +
        s"mode, this is a bug, please file an issue.")
    }
    super.explainTerms(pw).item("interval", interval)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamPlanner, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[BaseRow] = {
    val inputTransformation = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[BaseRow]]

    val inferredInterval = getTraitSet.getTrait(
      MiniBatchIntervalTraitDef.INSTANCE).getMiniBatchInterval

    val operator = if (inferredInterval.mode == MiniBatchMode.ProcTime) {
      new MiniBatchAssignerOperator(inferredInterval.interval)
    } else {
      throw new TableException(s"MiniBatchAssigner shouldn't be in ${inferredInterval.mode} " +
        s"mode, this is a bug, please file an issue.")
    }

    val outputRowTypeInfo = BaseRowTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType))
    val transformation = new OneInputTransformation[BaseRow, BaseRow](
      inputTransformation,
      getRelDetailedDescription,
      operator,
      outputRowTypeInfo,
      inputTransformation.getParallelism)
    transformation
  }

}


