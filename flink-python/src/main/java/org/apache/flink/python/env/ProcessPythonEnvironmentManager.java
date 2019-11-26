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

package org.apache.flink.python.env;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.python.util.ResourceUtil;
import org.apache.flink.python.util.UnzipUtil;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.ShutdownHookUtil;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Environments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * The ProcessPythonEnvironmentManager is used to prepare the working dir of python UDF worker and create
 * ProcessEnvironment object of Beam Fn API. It's used when the python function runner is configured to run python UDF
 * in process mode.
 */
@Internal
public final class ProcessPythonEnvironmentManager implements PythonEnvironmentManager {

	private static final Logger LOG = LoggerFactory.getLogger(ProcessPythonEnvironmentManager.class);

	@VisibleForTesting
	public static final String PYTHON_REQUIREMENTS_FILE = "_PYTHON_REQUIREMENTS_FILE";
	@VisibleForTesting
	public static final String PYTHON_REQUIREMENTS_CACHE = "_PYTHON_REQUIREMENTS_CACHE";
	@VisibleForTesting
	public static final String PYTHON_REQUIREMENTS_INSTALL_DIR = "_PYTHON_REQUIREMENTS_INSTALL_DIR";
	@VisibleForTesting
	public static final String PYTHON_WORKING_DIR = "_PYTHON_WORKING_DIR";

	@VisibleForTesting
	static final String PYTHON_REQUIREMENTS_DIR = "python-requirements";
	@VisibleForTesting
	static final String PYTHON_ARCHIVES_DIR = "python-archives";
	@VisibleForTesting
	static final String PYTHON_FILES_DIR = "python-files";

	private final PythonDependencyInfo dependencyInfo;
	private String baseDirectory;

	/**
	 * Directory for storing the install result of requirements list file (requirements.txt).
	 */
	private String requirementsDirectory;

	/**
	 * Directory for storing the extract result of archive files.
	 */
	private String archivesDirectory;

	/**
	 * Directory for storing the uploaded python libraries.
	 */
	private String filesDirectory;

	private final Map<String, String> systemEnv;
	private final String[] tmpDirectories;
	@Nullable private final String logDirectory;
	private Thread shutdownHook;

	public ProcessPythonEnvironmentManager(
		PythonDependencyInfo dependencyInfo,
		String[] tmpDirectories,
		@Nullable String logDirectory,
		Map<String, String> systemEnv) {
		this.dependencyInfo = dependencyInfo;
		this.tmpDirectories = tmpDirectories;
		this.logDirectory = logDirectory;
		this.systemEnv = systemEnv;
	}

	@Override
	public void open() throws Exception {
		// The base directory of all the generated files.
		baseDirectory = createBaseDirectory(tmpDirectories);
		// Archives directory is used to store the extract result of python archives.
		archivesDirectory = String.join(File.separator, baseDirectory, PYTHON_ARCHIVES_DIR);
		// Requirements directory is used to store the install result of requirements file.
		requirementsDirectory = String.join(File.separator, baseDirectory, PYTHON_REQUIREMENTS_DIR);
		// Files directory is used to store the python files or their symbolic links which will
		// be added to the PYTHONPATH of udf worker.
		filesDirectory = String.join(File.separator, baseDirectory, PYTHON_FILES_DIR);

		File baseDirectoryFile = new File(baseDirectory);
		if (!baseDirectoryFile.exists() && !baseDirectoryFile.mkdir()) {
			throw new IOException(
				"Can not create the base directory: " + baseDirectory + "of python process environment manager!");
		}
		shutdownHook = ShutdownHookUtil.addShutdownHook(
			this, ProcessPythonEnvironmentManager.class.getSimpleName(), LOG);
	}

	@Override
	public void close() {
		FileUtils.deleteDirectoryQuietly(new File(baseDirectory));
		if (shutdownHook != null) {
			ShutdownHookUtil.removeShutdownHook(
				shutdownHook, ProcessPythonEnvironmentManager.class.getSimpleName(), LOG);
			shutdownHook = null;
		}
	}

	@Override
	public RunnerApi.Environment createEnvironment() throws IOException, InterruptedException {
		prepareWorkingDir();
		Map<String, String> generatedEnv = generateEnvironmentVariables();
		String pythonWorkerCommand = String.join(File.separator, baseDirectory, "pyflink-udf-runner.sh");

		return Environments.createProcessEnvironment(
			"",
			"",
			pythonWorkerCommand,
			generatedEnv);
	}

	/**
	 * Just return a empty RetrievalToken because no files will be transmit via ArtifactService in process mode.
	 *
	 * @return The path of empty RetrievalToken.
	 */
	@Override
	public String createRetrievalToken() throws IOException {
		File retrievalToken = new File(baseDirectory,
			"retrieval_token_" + UUID.randomUUID().toString() + ".json");
		if (retrievalToken.createNewFile()) {
			final DataOutputStream dos = new DataOutputStream(new FileOutputStream(retrievalToken));
			dos.writeBytes("{\"manifest\": {}}");
			dos.flush();
			dos.close();
			return retrievalToken.getAbsolutePath();
		} else {
			throw new IOException(
				"Could not create the RetrievalToken file: " + retrievalToken.getAbsolutePath());
		}
	}

	/**
	 * Generate the environment variables used to create the launcher process of python UDF worker.
	 *
	 * <p>To avoid unnecessary IO usage, when running in process mode no artifacts will be transmitted
	 * via ArtifactService of Beam. Instead, the path of artifacts will be transmit to the launcher
	 * of python UDF worker via environment variable.
	 *
	 * @return Environment variable map containing paths of python dependencies.
	 */
	@VisibleForTesting
	Map<String, String> generateEnvironmentVariables() {
		Map<String, String> systemEnv = new HashMap<>(this.systemEnv);

		// add pyflink, py4j and cloudpickle to PYTHONPATH
		String internalLibs = Arrays.stream(ResourceUtil.PYTHON_BASIC_DEPENDENCIES)
			.filter(file -> file.endsWith(".zip"))
			.map(file -> String.join(File.separator, baseDirectory, file))
			.collect(Collectors.joining(File.pathSeparator));
		appendToPythonPath(systemEnv, internalLibs);

		// if the log directory exists, transmit it to the worker
		if (logDirectory != null && !logDirectory.isEmpty()) {
			systemEnv.put("FLINK_LOG_DIR", logDirectory);
		}

		// generate the PYTHONPATH of udf worker.
		if (!dependencyInfo.getPythonFiles().isEmpty()) {
			List<String> filesList = new ArrayList<>();
			for (Map.Entry<String, String> entry : dependencyInfo.getPythonFiles().entrySet()) {
				// The origin file name will be wiped during the transmission in Flink Distributed Cache
				// and replaced by a unreadable character sequence, now restore their origin name.
				// Every python file will be placed at :
				// ${baseDirectory}/python_files_dir/${distributedCacheFileName}/${originFileName}
				String distributedCacheFileName = new File(entry.getKey()).getName();
				String actualFileName = entry.getValue();
				File file = new File(entry.getKey());
				String pathForSearching;
				if (file.isFile() && actualFileName.endsWith(".py")) {
					// If the file is single py file, use its parent directory as PYTHONPATH.
					pathForSearching = String.join(File.separator, filesDirectory, distributedCacheFileName);
				} else {
					pathForSearching = String.join(
						File.separator, filesDirectory, distributedCacheFileName, actualFileName);
				}
				filesList.add(pathForSearching);
			}
			appendToPythonPath(systemEnv, String.join(File.pathSeparator, filesList));
		}
		LOG.info("PYTHONPATH of python worker: {}", systemEnv.get("PYTHONPATH"));

		// To support set python interpreter path in archives, the archives directory should be used as
		// working directory of udf worker.
		if (!dependencyInfo.getArchives().isEmpty()) {
			systemEnv.put(PYTHON_WORKING_DIR, archivesDirectory);
			LOG.info("Python working dir of python worker: {}", archivesDirectory);
		}

		// The requirements will be installed by a bootstrap script, here just transmit the necessary information
		// to the script via environment variable.
		if (dependencyInfo.getRequirementsFilePath().isPresent()) {
			systemEnv.put(PYTHON_REQUIREMENTS_FILE, dependencyInfo.getRequirementsFilePath().get());
			LOG.info("Requirements.txt of python worker: {}", dependencyInfo.getRequirementsFilePath().get());

			if (dependencyInfo.getRequirementsCacheDir().isPresent()) {
				systemEnv.put(PYTHON_REQUIREMENTS_CACHE, dependencyInfo.getRequirementsCacheDir().get());
				LOG.info("Requirements cached dir of python worker: {}",
					dependencyInfo.getRequirementsCacheDir().get());
			}

			systemEnv.put(PYTHON_REQUIREMENTS_INSTALL_DIR, requirementsDirectory);
			LOG.info("Requirements install directory of python worker: {}", requirementsDirectory);
		}

		// Transmit the path of python interpreter to boot script.
		if (dependencyInfo.getPythonExec().isPresent()) {
			systemEnv.put("python", dependencyInfo.getPythonExec().get());
			LOG.info("User defined python interpreter path: {}", dependencyInfo.getPythonExec());
		}
		return systemEnv;
	}

	/**
	 * Prepare the working directory for running the python UDF worker.
	 *
	 * <p>For the files to append to PYTHONPATH directly, restore their origin file names.
	 * For the files used for pip installations, prepare the target directory to contain the install result.
	 * For the archive files, extract them to their target directory and try to restore their origin permissions.
	 */
	@VisibleForTesting
	void prepareWorkingDir() throws IOException, IllegalArgumentException, InterruptedException {
		// Extract internal python libs and udf runner script.
		ResourceUtil.extractBasicDependenciesFromResource(
			baseDirectory,
			"",
			false);

		for (Map.Entry<String, String> entry : dependencyInfo.getPythonFiles().entrySet()) {
			// The origin file name will be wiped during the transmission in Flink Distributed Cache
			// and replaced by a unreadable character sequence, now restore their origin name.
			// Every python file will be placed at :
			// ${baseDirectory}/python_files_dir/${distributedCacheFileName}/${originFileName}
			String distributedCacheFileName = new File(entry.getKey()).getName();
			String actualFileName = entry.getValue();
			Path target = FileSystems.getDefault().getPath(filesDirectory,
				distributedCacheFileName, actualFileName);
			if (!target.getParent().toFile().mkdirs()) {
				throw new IllegalArgumentException(
					String.format("Can not create the directory: %s !", target.getParent().toString()));
			}
			Path src = FileSystems.getDefault().getPath(entry.getKey());
			try {
				Files.createSymbolicLink(target, src);
			} catch (IOException e) {
				LOG.warn(String.format("Can not create the symbolic link of: %s, "
					+ "the link path is %s, fallback to copy.", src, target), e);
				FileUtils.copy(new org.apache.flink.core.fs.Path(src.toUri()),
					new org.apache.flink.core.fs.Path(target.toUri()), false);
			}
		}

		for (Map.Entry<String, String> entry : dependencyInfo.getArchives().entrySet()) {
			UnzipUtil.extractZipFileWithPermissions(
				entry.getKey(), String.join(File.separator, archivesDirectory, entry.getValue()));
		}

		File requirementsDirectoryFile = new File(requirementsDirectory);
		if (!requirementsDirectoryFile.exists()) {
			if (!requirementsDirectoryFile.mkdirs()) {
				throw new IllegalArgumentException(
					String.format("Creating the requirements target directory: %s failed!",
						requirementsDirectory));
			}
		} else {
			if (!requirementsDirectoryFile.isDirectory()) {
				throw new IllegalArgumentException(
					String.format("The requirements target directory path: %s is not a directory!",
						requirementsDirectory));
			}
		}
	}

	@VisibleForTesting
	String getBaseDirectory() {
		return baseDirectory;
	}

	private static void appendToPythonPath(Map<String, String> env, String libs) {
		String systemPythonPath = env.get("PYTHONPATH");
		if (systemPythonPath != null && !systemPythonPath.isEmpty()) {
			libs = String.join(File.pathSeparator, libs, systemPythonPath);
		}
		if (!libs.isEmpty()) {
			env.put("PYTHONPATH", libs);
		}
	}

	private static String createBaseDirectory(String[] tmpDirectories) throws IOException {
		Random rnd = new Random();
		// try to find a unique file name for the base directory
		int maxAttempts = 10;
		for (int attempt = 0; attempt < maxAttempts; attempt++) {
			String directory = tmpDirectories[rnd.nextInt(tmpDirectories.length)];
			File baseDirectory = new File(directory, "python-dist-" + UUID.randomUUID().toString());
			if (baseDirectory.mkdirs()) {
				return baseDirectory.getAbsolutePath();
			}
		}

		throw new IOException(
			"Could not find a unique directory name in '" + Arrays.toString(tmpDirectories) +
				"' for storing the generated files of python dependency.");
	}

	public static ProcessPythonEnvironmentManager create(
		PythonDependencyInfo pythonDependencyInfo,
		String[] tmpDirectories,
		@Nullable String logDirectory,
		Map<String, String> environmentVariable) {

		return new ProcessPythonEnvironmentManager(
			pythonDependencyInfo,
			tmpDirectories,
			logDirectory,
			environmentVariable);
	}
}
