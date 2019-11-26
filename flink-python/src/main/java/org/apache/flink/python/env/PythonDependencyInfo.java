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
import org.apache.flink.api.common.cache.DistributedCache;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * This class is used to parse the information of python dependency and environment management
 * stored in GlobalJobParameters. The parse result will be used to create PythonEnvironmentManager.
 */
@Internal
public final class PythonDependencyInfo {

	public static final String PYTHON_FILES = "python.files";
	public static final String PYTHON_REQUIREMENTS_FILE = "python.requirements-file";
	public static final String PYTHON_REQUIREMENTS_CACHE = "python.requirements-cache";
	public static final String PYTHON_ARCHIVES = "python.archives";
	public static final String PYTHON_EXEC = "python.exec";

	@Nonnull private final Map<String, String> pythonFiles;
	@Nullable private final String requirementsFilePath;
	@Nullable private final String requirementsCacheDir;
	@Nullable private final String pythonExec;
	@Nonnull private final Map<String, String> archives;

	/**
	 * @param pythonFiles Local path and origin file name of user python files uploaded by
	 *                    TableEnvironment#add_python_file() or command option "-pyfs". Key is local absolute path and
	 *                    value is origin file name.
	 * @param requirementsFilePath The file path of requirements.txt file specified by
	 *                             TableEnvironment#set_python_requirements() or command option "-pyreq".
	 * @param requirementsCacheDir The path of the directory uploaded by TableEnvironment#set_python_requirements()
	 *                            or command option "-pyreq". It is used to support installing python packages offline.
	 *                            If exists it should contain all the packages listed in requirements.txt file.
	 * @param pythonExec The path of python interpreter (e.g. /usr/local/bin/python).
	 *                   It can be specified by TableConfig#set_python_executable() or command option "-pyexec".
	 * @param archives Local path and target directory name of user python archives uploaded by
	 *                 TableEnvironment#add_python_archive() or command option "-pyarch".
	 *                 Key is local absolute path and value is target directory name.
	 */
	public PythonDependencyInfo(
		Map<String, String> pythonFiles,
		String requirementsFilePath,
		String requirementsCacheDir,
		String pythonExec,
		Map<String, String> archives) {
		this.pythonFiles = Objects.requireNonNull(pythonFiles);
		this.requirementsFilePath = requirementsFilePath;
		this.requirementsCacheDir = requirementsCacheDir;
		this.pythonExec = pythonExec;
		this.archives = Objects.requireNonNull(archives);
	}

	/**
	 * @return Local path and origin file name of user python files uploaded by
	 * 	       TableEnvironment#add_python_file() or command option "-pyfs".
	 * 	       Key is local absolute path and value is origin file name.
	 */
	public Map<String, String> getPythonFiles() {
		return pythonFiles;
	}

	/**
	 * @return The file path of requirements.txt file specified by
	 * 	       TableEnvironment#set_python_requirements() or command option "-pyreq".
	 */
	public Optional<String> getRequirementsFilePath() {
		return Optional.ofNullable(requirementsFilePath);
	}

	/**
	 * @return The path of the directory uploaded by TableEnvironment#set_python_requirements()
	 * 	       or command option "-pyreq". It is used to support installing python packages
	 * 	       offline.
	 * 	       If exists it should contain all the packages listed in requirements.txt file.
	 */
	public Optional<String> getRequirementsCacheDir() {
		return Optional.ofNullable(requirementsCacheDir);
	}

	/**
	 * @return The path of python interpreter (e.g. /usr/local/bin/python).
	 * 	       It can be specified by TableConfig#set_python_executable() or command option "-pyexec".
	 */
	public Optional<String> getPythonExec() {
		return Optional.ofNullable(pythonExec);
	}

	/**
	 * @return Local path and target directory name of user python archives uploaded by
	 * 	       TableEnvironment#add_python_archive() or command option "-pyarch".
	 * 	       Key is local absolute path and value is target directory name.
	 */
	public Map<String, String> getArchives() {
		return archives;
	}

	/**
	 * Creates PythonDependencyInfo from GlobalJobParameters and DistributedCache.
	 *
	 * @param globalJobParameters The parameter map which contains information of python dependency.
	 *                           Usually it is the map of GlobalJobParameters.
	 * @param distributedCache The DistributedCache object of current task.
	 * @return The PythonDependencyInfo object that contains whole information of python dependency.
	 */
	public static PythonDependencyInfo create(
		Map<String, String> globalJobParameters, DistributedCache distributedCache)
		throws IOException {
		ObjectMapper mapper = new ObjectMapper();

		Map<String, String> filesPathToFilesName = new HashMap<>();
		if (globalJobParameters.containsKey(PYTHON_FILES)) {
			Map<String, String> filesIdToFilesName =
				mapper.readValue(globalJobParameters.get(PYTHON_FILES), HashMap.class);
			for (Map.Entry<String, String> entry: filesIdToFilesName.entrySet()) {
				File pythonFile = distributedCache.getFile(entry.getKey());
				String filePath = pythonFile.getAbsolutePath();
				filesPathToFilesName.put(filePath, entry.getValue());
			}
		}

		String requirementsFilePath = null;
		String requirementsCacheDir = null;
		if (globalJobParameters.containsKey(PYTHON_REQUIREMENTS_FILE)) {
			requirementsFilePath = distributedCache.getFile(
				globalJobParameters.get(PYTHON_REQUIREMENTS_FILE)).getAbsolutePath();
			if (globalJobParameters.containsKey(PYTHON_REQUIREMENTS_CACHE)) {
				requirementsCacheDir = distributedCache.getFile(
					globalJobParameters.get(PYTHON_REQUIREMENTS_CACHE)).getAbsolutePath();
			}
		}

		Map<String, String> archives = new HashMap<>();
		if (globalJobParameters.containsKey(PYTHON_ARCHIVES)) {
			Map<String, String> archivesMap =
				mapper.readValue(globalJobParameters.get(PYTHON_ARCHIVES), HashMap.class);

			for (Map.Entry<String, String> entry: archivesMap.entrySet()) {
				String zipFilePath = distributedCache.getFile(entry.getKey()).getAbsolutePath();
				String targetPath = entry.getValue();
				archives.put(zipFilePath, targetPath);
			}
		}

		String pythonExec = globalJobParameters.get(PYTHON_EXEC);

		return new PythonDependencyInfo(
			filesPathToFilesName,
			requirementsFilePath,
			requirementsCacheDir,
			pythonExec,
			archives);
	}
}
