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

package org.apache.flink.kubernetes;

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.entrypoint.KubernetesSessionClusterEntrypoint;
import org.apache.flink.kubernetes.kubeclient.Endpoint;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.decorators.FlinkMasterDeploymentDecorator;
import org.apache.flink.kubernetes.kubeclient.resources.FlinkService;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

/**
 * Kubernetes specific {@link ClusterDescriptor} implementation.
 */
public class KubernetesClusterDescriptor implements ClusterDescriptor<String> {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesClusterDescriptor.class);

	private static final String CLUSTER_DESCRIPTION = "Kubernetes cluster";

	private final Configuration flinkConfig;

	private final FlinkKubeClient client;

	private final String clusterId;

	public KubernetesClusterDescriptor(Configuration flinkConfig, FlinkKubeClient client) {
		this.flinkConfig = flinkConfig;
		this.client = client;
		this.clusterId = flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID);
		Preconditions.checkNotNull(clusterId, "ClusterId must be specified!");
	}

	@Override
	public String getClusterDescription() {
		return CLUSTER_DESCRIPTION;
	}

	private ClusterClient<String> createClusterClient(String clusterId) throws Exception {

		Configuration configuration = new Configuration(flinkConfig);

		Endpoint restEndpoint = this.client.getRestEndpoints(clusterId);

		if (restEndpoint != null) {
			configuration.setString(RestOptions.ADDRESS, restEndpoint.getAddress());
			configuration.setInteger(RestOptions.PORT, restEndpoint.getPort());
		} else {
			throw new ClusterRetrieveException("Could not get the rest endpoint of " + clusterId);
		}

		return new RestClusterClient<>(configuration, clusterId);
	}

	@Override
	public ClusterClient<String> retrieve(String clusterId) throws ClusterRetrieveException {
		try {
			ClusterClient<String> retrievedClient = this.createClusterClient(clusterId);
			LOG.info("Retrieve flink cluster {} successfully, JobManager Web Interface : {}", clusterId,
				retrievedClient.getWebInterfaceURL());
			return retrievedClient;
		} catch (Exception e) {
			this.client.handleException(e);
			throw new ClusterRetrieveException("Could not create the RestClusterClient.", e);
		}
	}

	@Override
	public ClusterClient<String> deploySessionCluster(ClusterSpecification clusterSpecification)
		throws ClusterDeploymentException {

		ClusterClient<String> clusterClient = this.deployClusterInternal(
			KubernetesSessionClusterEntrypoint.class.getName(), clusterSpecification, false);

		LOG.info("Create flink session cluster {} successfully, JobManager Web Interface: {}",
			clusterId, clusterClient.getWebInterfaceURL());
		return clusterClient;
	}

	@Override
	public ClusterClient<String> deployJobCluster(ClusterSpecification clusterSpecification,
			JobGraph jobGraph, boolean detached) throws ClusterDeploymentException {
		throw new ClusterDeploymentException("Per job could not be supported now.");
	}

	@Nonnull
	private ClusterClient<String> deployClusterInternal(String entryPoint,
			ClusterSpecification clusterSpecification,
			boolean detached) throws ClusterDeploymentException {

		final ClusterEntrypoint.ExecutionMode executionMode = detached ?
			ClusterEntrypoint.ExecutionMode.DETACHED
			: ClusterEntrypoint.ExecutionMode.NORMAL;
		flinkConfig.setString(ClusterEntrypoint.EXECUTION_MODE, executionMode.toString());

		flinkConfig.setString(FlinkMasterDeploymentDecorator.ENTRY_POINT_CLASS, entryPoint);

		// Rpc(6123), blob(6124), rest(8081) taskManagerRpc(6122) port need to be exposed, so update them to fixed port.
		flinkConfig.setString(BlobServerOptions.PORT, String.valueOf(Constants.BLOB_SERVER_PORT));
		flinkConfig.setString(TaskManagerOptions.RPC_PORT, String.valueOf(Constants.TASK_MANAGER_RPC_PORT));

		// Set jobmanager address to namespaced service name
		String nameSpace = flinkConfig.getString(KubernetesConfigOptions.NAMESPACE);
		flinkConfig.setString(JobManagerOptions.ADDRESS, clusterId + "." + nameSpace);

		try {
			FlinkService internalSvc = this.client.createInternalService(clusterId).get();
			// Update the service id in flink config, it will be used for gc.
			String serviceId = internalSvc.getInternalResource().getMetadata().getUid();
			if (serviceId != null) {
				flinkConfig.setString(FlinkService.SERVICE_ID, serviceId);
			} else {
				throw new ClusterDeploymentException("Get service id failed.");
			}

			// Create the rest service when exposed type is not ClusterIp.
			String restSvcExposedType = flinkConfig.getString(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE);
			if (!restSvcExposedType.equals(KubernetesConfigOptions.ServiceExposedType.ClusterIP.toString())) {
				this.client.createRestService(clusterId).get();
			}

			this.client.createConfigMap();
			this.client.createFlinkMasterDeployment(clusterSpecification);

			return this.createClusterClient(clusterId);
		} catch (Exception e) {
			this.client.handleException(e);
			throw new ClusterDeploymentException("Could not create Kubernetes cluster " + clusterId, e);
		}
	}

	@Override
	public void killCluster(String clusterId) throws FlinkException {
		try {
			this.client.stopAndCleanupCluster(clusterId);
		} catch (Exception e) {
			this.client.handleException(e);
			throw new FlinkException("Could not kill Kubernetes cluster " + clusterId);
		}
	}

	@Override
	public void close() {
		try {
			this.client.close();
		} catch (Exception e) {
			this.client.handleException(e);
			LOG.error("failed to close client, exception {}", e.toString());
		}
	}
}