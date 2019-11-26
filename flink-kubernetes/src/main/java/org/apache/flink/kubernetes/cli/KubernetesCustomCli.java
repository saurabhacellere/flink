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
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.FlinkKubernetesOptions;
import org.apache.flink.kubernetes.cluster.KubernetesClusterDescriptor;
import org.apache.flink.kubernetes.kubeclient.KubeClient;
import org.apache.flink.kubernetes.kubeclient.KubeClientFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.client.cli.CliFrontendParser.DETACHED_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.DYNAMIC_PROPERTY_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.HOST_OPTION;
import static org.apache.flink.runtime.entrypoint.parser.CommandLineOptions.REST_PORT_OPTION;

/**
 * Kubernetes customized commandline.
 * */
public class KubernetesCustomCli extends AbstractCustomCommandLine<String> {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesCustomCli.class);

	// actions for commandline
	private static final String ID = "kubernetes-cluster";
	private static final String ACTION_START = "start";
	private static final String ACTION_LIST = "list";
	private static final String ACTION_STOP = "stop";

	private Properties dynamicProperties;

	private final Option dynamic;
	private final Option help;
	private final Option jar;
	private final Option id;

	public KubernetesCustomCli(Configuration configuration, String prefix) {
		super(configuration);

		dynamic = Option.builder(prefix + "D").argName("property=value").numberOfArgs(2)
			.valueSeparator('=').desc("use value for given property").build();

		help = Option.builder(prefix + "h").longOpt("help").hasArg(false)
			.desc("Help for Kubernetes session CLI.").build();

		jar = Option.builder(prefix + "j").longOpt("jar").hasArg(true)
			.desc("Path to Flink jar file").build();

		id = Option.builder(prefix + "id").longOpt("clusterId").hasArg(true)
			.desc("the cluster id that will be used for current session.").build();
	}

	public Properties getDynamicProperties() {
		return dynamicProperties;
	}

	/**
	 * active if commandline contains option -m kubernetes-cluster | -kid .
	 */
	@Override
	public boolean isActive(CommandLine commandLine) {
		String jobManagerOption = commandLine.getOptionValue(addressOption.getOpt(), null);
		boolean k8sJobManager = ID.equals(jobManagerOption);
		boolean k8sClusterId = commandLine.hasOption(id.getOpt());
		return k8sJobManager || k8sClusterId;
	}

	@Override
	public void addRunOptions(Options baseOptions) {
		baseOptions.addOption(FlinkKubernetesOptions.IMAGE_OPTION)
			.addOption(DETACHED_OPTION)
			.addOption(dynamic)
			.addOption(help)
			.addOption(jar)
			.addOption(id);
	}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public ClusterDescriptor<String> createClusterDescriptor(CommandLine commandLine) throws FlinkException {
		try {
			FlinkKubernetesOptions options = fromCommandLine(commandLine);
			addBackConfigurations(options, this.configuration);

			dynamicProperties = commandLine.getOptionProperties(DYNAMIC_PROPERTY_OPTION.getOpt());

			return new KubernetesClusterDescriptor(options);
		} catch (Exception e) {
			throw new FlinkException("Could not create the KubernetesClusterDescriptor.", e);
		}
	}

	@Nullable
	@Override
	public String getClusterId(CommandLine commandLine) {
		try {
			FlinkKubernetesOptions options = fromCommandLine(commandLine);

			//
			addBackConfigurations(options, this.configuration);

			if (options.getClusterId() != null) {
				return options.getClusterId();
			} else {
				KubeClient client = KubeClientFactory.fromConfiguration(options);

				List<String> clusterList = client.listFlinkClusters();

				if (clusterList != null && clusterList.size() > 0) {
					return clusterList.get(0);
				}
			}
		} catch (Exception e) {
			LOG.error("Could not retrieval cluster Id: {}.", e);
		}

		return null;
	}

	@Override
	public ClusterSpecification getClusterSpecification(CommandLine commandLine) {
		return null;
	}

	///////// Command line ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	public int processStartAction(String[] args) throws CliArgsException, FlinkException {
		//
		//	Command Line Options
		//
		final CommandLine cmd = parseCommandLineOptions(args, true);

		if (cmd.hasOption(help.getOpt())) {
			printUsage();
			return 0;
		}

		System.out.println("Starting K8s session...");

		ClusterDescriptor<String> cluster = this.createClusterDescriptor(cmd);
		final ClusterSpecification clusterSpecification = getClusterSpecification(cmd);
		ClusterClient<String> clusterClient = cluster.deploySessionCluster(clusterSpecification);
		Configuration config = clusterClient.getFlinkConfiguration();
		System.out.println("==============================================");

		String url = String.format("http://%s:%d/#/overview"
			, config.getString(JobManagerOptions.ADDRESS)
			, config.getInteger(RestOptions.PORT));

		System.out.println("Cluster " + clusterClient.getClusterId() + " started, web portal: " + url);

		try {
			System.out.println("Waiting for Job manager starting");
			Thread.sleep(5000);
			Runtime rt = Runtime.getRuntime();
			rt.exec("open " + url);
		} catch (Exception e) {
			System.out.println(e);
		}

		return 0;
	}

	public int processStopAction(String[] args) throws CliArgsException, FlinkException {
		//
		//	Command Line Options
		//
		System.out.println("Begin to stop K8s session");

		final CommandLine cmd = parseCommandLineOptions(args, true);

		if (cmd.hasOption(help.getOpt())) {
			printUsage();
			return 0;
		}

		String clusterId = getClusterId(cmd);

		if (clusterId == null) {
			System.out.println("No cluster id in stop command found. Exit.");
			return -1;
		}

		System.out.println("Stopping K8s session: " + clusterId);

		KubernetesClusterDescriptor cluster = (KubernetesClusterDescriptor) this.createClusterDescriptor(cmd);
		cluster.killCluster(clusterId);

		return 0;
	}

	public int processListAction(String[] args) throws CliArgsException, FlinkException {
		//
		//	Command Line Options
		//
		final CommandLine cmd = parseCommandLineOptions(args, true);

		if (cmd.hasOption(help.getOpt())) {
			printUsage();
			return 0;
		}

		return 0;
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
		addGeneralOptions(options);
		addRunOptions(options);
		formatter.printHelp(" ", options);
	}

	/**
	 * Parses the command line arguments and starts the requested action.
	 *
	 * @param args command line arguments of the client.
	 * @return The return code of the program
	 */
	public int parseParameters(String[] args) {

		// check for action
		if (args.length < 1) {
			System.out.println("Please specify an action.");
			return 1;
		}

		// get action
		String action = args[0];

		// remove action from parameters
		final String[] params = Arrays.copyOfRange(args, 1, args.length);

		try {
			// do action
			switch (action) {
				case ACTION_START:
					this.processStartAction(params);
					return 0;
				case ACTION_LIST:
					this.processListAction(params);
					return 0;
				case ACTION_STOP:
					this.processStopAction(params);
					return 0;
				case "--version":
					String version = EnvironmentInformation.getVersion();
					String commitID = EnvironmentInformation.getRevisionInformation().commitId;
					System.out.print("Version: " + version);
					System.out.println(commitID.equals(EnvironmentInformation.UNKNOWN) ? "" : ", Commit ID: " + commitID);
					return 0;
				default:
					System.out.printf("\"%s\" is not a valid action.\n", action);
					System.out.println();
					System.out.println("Valid actions are \"start\", \"list\", or \"stop\".");
					System.out.println();
					System.out.println("Specify the version option (-v or --version) to print Flink version.");
					System.out.println();
					System.out.println("Specify the help option (-h or --help) to get help on the command.");
					return 1;
			}
		} catch (CliArgsException ce) {
			return handleCliArgsException(ce);
		} catch (Exception e) {
			return handleError(e);
		}
	}

	public static void main(String[] args) {

		final Configuration configuration = GlobalConfiguration.loadConfiguration();

		int retCode;

		try {
			final KubernetesCustomCli cli = new KubernetesCustomCli(configuration, "");
			retCode = cli.parseParameters(args);
		} catch (Throwable t) {
			final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
			retCode = handleError(strippedThrowable);
		}

		System.exit(retCode);
	}

	/**
	 * Add the key-value in conf to the configuration in flinkOptions.
	 * Overwrite the value if the same key was found in flinkOptions.
	 * @param conf
	 * @param flinkOptions
	 */
	private void addBackConfigurations(FlinkKubernetesOptions flinkOptions, Configuration conf) {
		Configuration tmpConfig = flinkOptions.getConfiguration();
		flinkOptions.getConfiguration().addAll(conf);
		flinkOptions.getConfiguration().addAll(tmpConfig);
	}

	private static int handleCliArgsException(CliArgsException e) {
		LOG.error("Could not parse the command line arguments.", e);

		System.out.println(e.getMessage());
		System.out.println();
		System.out.println("Use the help option (-h or --help) to get help on the command.");
		return 1;
	}

	private static int handleError(Throwable t) {
		LOG.error("Error while running the Flink Yarn session.", t);

		System.err.println();
		System.err.println("------------------------------------------------------------");
		System.err.println(" The program finished with the following exception:");
		System.err.println();

		t.printStackTrace();
		return 1;
	}

	/**
	 * build FlinkKubernetesOption from commandline.
	 * */
	protected FlinkKubernetesOptions fromCommandLine(CommandLine commandLine){
		final Properties dynamicProperties = commandLine.getOptionProperties(DYNAMIC_PROPERTY_OPTION.getOpt());
		final String restPortString = commandLine.getOptionValue(REST_PORT_OPTION.getOpt(), "-1");
		int restPort = Integer.parseInt(restPortString);
		String hostname = commandLine.getOptionValue(HOST_OPTION.getOpt());
		final String imageName = commandLine.getOptionValue(FlinkKubernetesOptions.IMAGE_OPTION.getOpt());
		final String clusterId = commandLine.getOptionValue(id.getOpt());

		//hostname = hostname == null ? clusterId : hostname;
		Configuration configuration = GlobalConfiguration
			.loadConfiguration(ConfigurationUtils.createConfiguration(dynamicProperties));

		if (hostname != null) {
			System.out.print("rest.address is: " + hostname);
			configuration.setString(RestOptions.ADDRESS, hostname);
		}

		if (restPort == -1) {
			restPort = RestOptions.PORT.defaultValue();
			configuration.setInteger(RestOptions.PORT, restPort);
		}

		FlinkKubernetesOptions options = new FlinkKubernetesOptions(configuration);
		options.setClusterId(clusterId);
		options.setImageName(imageName);

		return options;
	}
}
