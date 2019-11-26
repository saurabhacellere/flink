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

package org.apache.flink.kubernetes.cli;

import org.apache.flink.client.cli.AbstractCustomCommandLine;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.executors.KubernetesJobClusterExecutor;
import org.apache.flink.kubernetes.executors.KubernetesSessionClusterExecutor;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubeClientFactory;
import org.apache.flink.kubernetes.kubeclient.decorators.FlinkMasterDeploymentDecorator;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.apache.flink.client.cli.CliFrontendParser.DETACHED_OPTION;
import static org.apache.flink.configuration.HighAvailabilityOptions.HA_CLUSTER_ID;
import static org.apache.flink.kubernetes.cli.KubernetesCliOptions.CLUSTER_ID_OPTION;
import static org.apache.flink.kubernetes.cli.KubernetesCliOptions.DYNAMIC_PROPERTY_OPTION;
import static org.apache.flink.kubernetes.cli.KubernetesCliOptions.HELP_OPTION;
import static org.apache.flink.kubernetes.cli.KubernetesCliOptions.IMAGE_OPTION;
import static org.apache.flink.kubernetes.cli.KubernetesCliOptions.JOB_CLASS_NAME_OPTION;
import static org.apache.flink.kubernetes.cli.KubernetesCliOptions.JOB_ID_OPTION;

/**
 * Kubernetes customized commandline.
 */
public class FlinkKubernetesCustomCli extends AbstractCustomCommandLine {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKubernetesCustomCli.class);

	private static final long CLIENT_POLLING_INTERVAL_MS = 3000L;

	private static final String KUBERNETES_CLUSTER_HELP = "Available commands:\n" +
		"help - show these commands\n" +
		"stop - stop the kubernetes cluster\n" +
		"quit - quit attach mode";

	private static final String ID = "kubernetes-cluster";

	// Options with short prefix(k) or long prefix(kubernetes)
	private final Option imageOption;
	private final Option clusterIdOption;
	private final Option jobManagerMemeoryOption;
	private final Option taskManagerMemeoryOption;
	private final Option taskManagerSlotsOption;
	private final Option dynamicPropertiesOption;
	private final Option helpOption;

	private final Option jobClassOption;
	private final Option jobIdOption;

	private final ClusterClientServiceLoader clusterClientServiceLoader;

	public FlinkKubernetesCustomCli(
		Configuration configuration,
		String shortPrefix,
		String longPrefix) {
		this(configuration, new DefaultClusterClientServiceLoader(), shortPrefix, longPrefix);
	}

	public FlinkKubernetesCustomCli(
		Configuration configuration,
		ClusterClientServiceLoader clusterClientServiceLoader,
		String shortPrefix,
		String longPrefix) {
		super(configuration);

		this.clusterClientServiceLoader = clusterClientServiceLoader;

		this.imageOption = KubernetesCliOptions.getOptionWithPrefix(IMAGE_OPTION, shortPrefix, longPrefix);
		this.clusterIdOption = KubernetesCliOptions.getOptionWithPrefix(CLUSTER_ID_OPTION, shortPrefix, longPrefix);
		this.dynamicPropertiesOption = KubernetesCliOptions.getOptionWithPrefix(
			DYNAMIC_PROPERTY_OPTION, shortPrefix, longPrefix);

		this.jobManagerMemeoryOption = KubernetesCliOptions.getOptionWithPrefix(
			KubernetesCliOptions.JOB_MANAGER_MEMEORY_OPTION, shortPrefix, longPrefix);
		this.taskManagerMemeoryOption = KubernetesCliOptions.getOptionWithPrefix(
			KubernetesCliOptions.TASK_MANAGER_MEMEORY_OPTION, shortPrefix, longPrefix);
		this.taskManagerSlotsOption = KubernetesCliOptions.getOptionWithPrefix(
			KubernetesCliOptions.TASK_MANAGER_SLOTS_OPTION, shortPrefix, longPrefix);

		this.jobClassOption = KubernetesCliOptions.getOptionWithPrefix(JOB_CLASS_NAME_OPTION, shortPrefix, longPrefix);
		this.jobIdOption = KubernetesCliOptions.getOptionWithPrefix(JOB_ID_OPTION, shortPrefix, longPrefix);

		this.helpOption = KubernetesCliOptions.getOptionWithPrefix(HELP_OPTION, shortPrefix, longPrefix);
	}

	/**
	 * active if commandline contains option -m kubernetes-cluster or -kid.
	 */
	@Override
	public boolean isActive(CommandLine commandLine) {
		String jobManagerOption = commandLine.getOptionValue(addressOption.getOpt(), null);
		boolean k8sJobManager = ID.equals(jobManagerOption);
		boolean k8sClusterId = commandLine.hasOption(clusterIdOption.getOpt());
		return k8sJobManager || k8sClusterId;
	}

	@Override
	public void addRunOptions(Options baseOptions) {
		baseOptions.addOption(DETACHED_OPTION)
			.addOption(imageOption)
			.addOption(clusterIdOption)
			.addOption(jobManagerMemeoryOption)
			.addOption(taskManagerMemeoryOption)
			.addOption(taskManagerSlotsOption)
			.addOption(dynamicPropertiesOption)
			.addOption(helpOption);
	}

	@Override
	public void addGeneralOptions(Options baseOptions) {
		baseOptions.addOption(clusterIdOption);
	}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public Configuration applyCommandLineOptionsToConfiguration(CommandLine commandLine) {
		final Configuration effectiveConfiguration = new Configuration(configuration);

		effectiveConfiguration.setString(DeploymentOptions.TARGET, KubernetesJobClusterExecutor.NAME);
		if (commandLine.hasOption(clusterIdOption.getOpt())) {
			String clusterId = commandLine.getOptionValue(clusterIdOption.getOpt());
			effectiveConfiguration.setString(KubernetesConfigOptions.CLUSTER_ID, clusterId);
			effectiveConfiguration.setString(HA_CLUSTER_ID, clusterId);
			// If -m kubernetes-cluster is not specified, it means to submit a flink job to existed session.
			if (!commandLine.hasOption(addressOption.getOpt())) {
				effectiveConfiguration.setString(DeploymentOptions.TARGET, KubernetesSessionClusterExecutor.NAME);
			}
		}

		if (commandLine.hasOption(imageOption.getOpt())) {
			effectiveConfiguration.setString(KubernetesConfigOptions.CONTAINER_IMAGE,
				commandLine.getOptionValue(imageOption.getOpt()));
		}

		if (commandLine.hasOption(jobManagerMemeoryOption.getOpt())) {
			String jmMemoryVal = commandLine.getOptionValue(jobManagerMemeoryOption.getOpt());
			if (!MemorySize.MemoryUnit.hasUnit(jmMemoryVal)) {
				jmMemoryVal += "m";
			}
			effectiveConfiguration.setString(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY, jmMemoryVal);
		}

		if (commandLine.hasOption(taskManagerMemeoryOption.getOpt())) {
			String tmMemoryVal = commandLine.getOptionValue(taskManagerMemeoryOption.getOpt());
			if (!MemorySize.MemoryUnit.hasUnit(tmMemoryVal)) {
				tmMemoryVal += "m";
			}
			effectiveConfiguration.setString(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY, tmMemoryVal);
		}

		if (commandLine.hasOption(taskManagerSlotsOption.getOpt())) {
			effectiveConfiguration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS,
				Integer.parseInt(commandLine.getOptionValue(taskManagerSlotsOption.getOpt())));
		}

		Configuration dynamicConf = ConfigurationUtils.createConfiguration(
			commandLine.getOptionProperties(dynamicPropertiesOption.getOpt()));
		for (String key : dynamicConf.keySet()) {
			effectiveConfiguration.setString(key, dynamicConf.getString(key, ""));
		}

		StringBuilder entryPointClassArgs = new StringBuilder();
		if (commandLine.hasOption(jobClassOption.getOpt())) {
			entryPointClassArgs.append(" --")
				.append(jobClassOption.getLongOpt())
				.append(" ")
				.append(commandLine.getOptionValue(jobClassOption.getOpt()));
		}

		if (commandLine.hasOption(jobIdOption.getOpt())) {
			entryPointClassArgs.append(" --")
				.append(jobIdOption.getLongOpt())
				.append(" ")
				.append(commandLine.getOptionValue(jobIdOption.getOpt()));
		}
		if (!entryPointClassArgs.toString().isEmpty()) {
			effectiveConfiguration.setString(FlinkMasterDeploymentDecorator.ENTRY_POINT_CLASS_ARGS,
				entryPointClassArgs.toString());
		}

		return effectiveConfiguration;
	}

	private int run(String[] args) throws CliArgsException, FlinkException {
		final CommandLine cmd = parseCommandLineOptions(args, true);

		if (cmd.hasOption(HELP_OPTION.getOpt())) {
			printUsage();
			return 0;
		}

		final Configuration configuration = applyCommandLineOptionsToConfiguration(cmd);
		final ClusterClientFactory<String> kubernetesClusterClientFactory =
			clusterClientServiceLoader.getClusterClientFactory(configuration);

		final ClusterDescriptor<String> kubernetesClusterDescriptor =
			kubernetesClusterClientFactory.createClusterDescriptor(configuration);

		try {
			final ClusterClient<String> clusterClient;
			String clusterId = kubernetesClusterClientFactory.getClusterId(configuration);
			boolean detached = cmd.hasOption(DETACHED_OPTION.getOpt());
			FlinkKubeClient kubeClient = KubeClientFactory.fromConfiguration(configuration);

			// Retrieve or create a session cluster.
			if (clusterId != null && kubeClient.getInternalService(clusterId) != null) {
				clusterClient = kubernetesClusterDescriptor.retrieve(clusterId);
			} else {
				clusterClient = kubernetesClusterDescriptor.deploySessionCluster(
					kubernetesClusterClientFactory.getClusterSpecification(configuration));
				clusterId = clusterClient.getClusterId();
			}

			try {
				if (!detached) {
					boolean[] continueRepl = new boolean[] {true, false};
					try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
						while (continueRepl[0]) {
							continueRepl = repStep(in);
						}
					} catch (Exception e) {
						LOG.warn("Exception while running the interactive command line interface.", e);
					}
					if (continueRepl[1]) {
						kubernetesClusterDescriptor.killCluster(clusterId);
					}
				}
				clusterClient.close();
				kubeClient.close();
			} catch (Exception e) {
				LOG.info("Could not properly shutdown cluster client.", e);
			}
		} finally {
			try {
				kubernetesClusterDescriptor.close();
			} catch (Exception e) {
				LOG.info("Could not properly close the kubernetes cluster descriptor.", e);
			}
		}

		return 0;
	}

	/**
	 * res[0] whether need to continue read from input.
	 * res[1] whether need to kill the cluster.
	 */
	private boolean[] repStep(BufferedReader in) throws IOException, InterruptedException {

		// wait until CLIENT_POLLING_INTERVAL is over or the user entered something.
		long startTime = System.currentTimeMillis();
		while ((System.currentTimeMillis() - startTime) < CLIENT_POLLING_INTERVAL_MS
			&& (!in.ready())) {
			Thread.sleep(200L);
		}
		//------------- handle interactive command by user. ----------------------

		if (in.ready()) {
			String command = in.readLine();
			switch (command) {
				case "quit":
					return new boolean[] {false, false};
				case "stop":
					return new boolean[] {false, true};

				case "help":
					System.err.println(KUBERNETES_CLUSTER_HELP);
					break;
				default:
					System.err.println("Unknown command '" + command + "'. Showing help:");
					System.err.println(KUBERNETES_CLUSTER_HELP);
					break;
			}
		}

		return new boolean[] {true, false};
	}

	private void printUsage() {
		System.out.println("Usage:");
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(200);
		formatter.setLeftPadding(5);
		formatter.setSyntaxPrefix("   Required");
		Options req = new Options();
		formatter.printHelp(" ", req);

		formatter.setSyntaxPrefix("   Optional");
		Options options = new Options();
		addRunOptions(options);
		formatter.printHelp(" ", options);
	}

	public static void main(String[] args) {
		final Configuration configuration = GlobalConfiguration.loadConfiguration();

		int retCode;

		try {
			final FlinkKubernetesCustomCli cli = new FlinkKubernetesCustomCli(configuration, "", "");
			retCode = SecurityUtils.getInstalledContext().runSecured(() -> cli.run(args));
		} catch (CliArgsException e) {
			retCode = handleCliArgsException(e);
		} catch (Exception e) {
			retCode = handleError(e);
		}

		System.exit(retCode);
	}

	private static int handleCliArgsException(CliArgsException e) {
		LOG.error("Could not parse the kubernetes command line arguments.", e);

		System.out.println(e.getMessage());
		System.out.println();
		System.out.println("Use the help option (-h or --help) to get help on the command.");
		return 1;
	}

	private static int handleError(Throwable t) {
		LOG.error("Error while running the Flink kubernetes session.", t);

		System.err.println();
		System.err.println("------------------------------------------------------------");
		System.err.println(" The program finished with the following exception:");
		System.err.println();

		t.printStackTrace();
		return 1;
	}
}
