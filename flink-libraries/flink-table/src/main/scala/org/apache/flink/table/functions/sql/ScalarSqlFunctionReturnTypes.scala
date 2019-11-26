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
package org.apache.flink.table.functions.sql

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.SqlOperatorBinding
import org.apache.calcite.sql.`type`.{SqlReturnTypeInference, SqlTypeName}
import org.apache.flink.table.runtime.functions.DateTimeFunctions

/**
  * Customized [[SqlReturnTypeInference]] for those whose [[SqlReturnTypeInference]] cannot
  * be implemented with [[org.apache.calcite.sql.type.ReturnTypes]]
  */
object ScalarSqlFunctionReturnTypes {

  val STR_TO_DATE: SqlReturnTypeInference = new SqlReturnTypeInference() {
    override def inferReturnType(opBinding: SqlOperatorBinding): RelDataType = {
      val typeFactory = opBinding.getTypeFactory
      if (opBinding.isOperandLiteral(1, true)) {
        val format = opBinding.getOperandLiteralValue(1, classOf[String])
        DateTimeFunctions.checkOutputFormat(format) match {
          case DateTimeFunctions.ONLY_DATE_FORMAT =>
            typeFactory.createSqlType(SqlTypeName.DATE)
          case DateTimeFunctions.ONLY_TIME_FORMAT =>
            typeFactory.createSqlType(SqlTypeName.TIME)
          case DateTimeFunctions.DATETIME_FORMAT =>
            typeFactory.createSqlType(SqlTypeName.TIMESTAMP)
          case _ =>
            throw new RuntimeException(s"Unable to support format $format")
        }
      } else {
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP)
      }
    }
  }

}
