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

package org.apache.flink.table.catalog.hive.client;

import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.thrift.TException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Shim for Hive version 2.1.0.
 */
public class HiveShimV210 extends HiveShimV201 {

	@Override
	public void alterPartition(IMetaStoreClient client, String databaseName, String tableName, Partition partition)
			throws InvalidOperationException, MetaException, TException {
		String errorMsg = "Failed to alter partition for table %s in database %s";
		try {
			Method method = client.getClass().getMethod("alter_partition", String.class, String.class,
				Partition.class, EnvironmentContext.class);
			method.invoke(client, databaseName, tableName, partition, null);
		} catch (InvocationTargetException ite) {
			Throwable targetEx = ite.getTargetException();
			if (targetEx instanceof TException) {
				throw (TException) targetEx;
			} else {
				throw new CatalogException(String.format(errorMsg, tableName, databaseName), targetEx);
			}
		} catch (NoSuchMethodException | IllegalAccessException e) {
			throw new CatalogException(String.format(errorMsg, tableName, databaseName), e);
		}
	}

	@Override
	public List<String> getPrimaryKey(IMetaStoreClient client, String dbName, String tableName, byte constraintTrait) {
		try {
			Class requestClz = Class.forName("org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest");
			Object request = requestClz.getDeclaredConstructor(String.class, String.class).newInstance(dbName, tableName);
			List<?> constraints = (List<?>) HiveReflectionUtils.invokeMethod(client.getClass(), client,
					"getPrimaryKeys", new Class[]{requestClz}, new Object[]{request});
			Class constraintClz = Class.forName("org.apache.hadoop.hive.metastore.api.SQLPrimaryKey");
			Method colNameMethod = constraintClz.getDeclaredMethod("getColumn_name");
			Method isEnableMethod = constraintClz.getDeclaredMethod("isEnable_cstr");
			Method isValidateMethod = constraintClz.getDeclaredMethod("isValidate_cstr");
			Method isRelyMethod = constraintClz.getDeclaredMethod("isRely_cstr");
			List<String> res = new ArrayList<>();
			for (Object constraint : constraints) {
				boolean add = !HiveTableUtil.requireEnableConstraint(constraintTrait) || (boolean) isEnableMethod.invoke(constraint);
				if (add) {
					add = !HiveTableUtil.requireValidateConstraint(constraintTrait) || (boolean) isValidateMethod.invoke(constraint);
				}
				if (add) {
					add = !HiveTableUtil.requireRelyConstraint(constraintTrait) || (boolean) isRelyMethod.invoke(constraint);
				}
				if (add) {
					res.add((String) colNameMethod.invoke(constraint));
				} else {
					return Collections.emptyList();
				}
			}
			return res;
		} catch (Exception e) {
			throw new CatalogException("Failed to get PrimaryKey constraints", e);
		}
	}

}
