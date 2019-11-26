/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.python.env;

import org.apache.flink.util.FileUtils;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.flink.python.env.ProcessPythonEnvironmentManager.PYTHON_ARCHIVES_DIR;
import static org.apache.flink.python.env.ProcessPythonEnvironmentManager.PYTHON_FILES_DIR;
import static org.apache.flink.python.env.ProcessPythonEnvironmentManager.PYTHON_REQUIREMENTS_CACHE;
import static org.apache.flink.python.env.ProcessPythonEnvironmentManager.PYTHON_REQUIREMENTS_DIR;
import static org.apache.flink.python.env.ProcessPythonEnvironmentManager.PYTHON_REQUIREMENTS_FILE;
import static org.apache.flink.python.env.ProcessPythonEnvironmentManager.PYTHON_REQUIREMENTS_INSTALL_DIR;
import static org.apache.flink.python.env.ProcessPythonEnvironmentManager.PYTHON_WORKING_DIR;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link ProcessPythonEnvironmentManager}.
 */
public class ProcessPythonEnvironmentManagerTest {

	private static String tmpDir;
	private static Thread shutdownHook;
	private static boolean isMacOrUnix;

	@BeforeClass
	public static void prepareTempDirectory() throws IOException {
		File tmpFile = File.createTempFile("process_environment_manager_test", "");
		if (tmpFile.delete() && tmpFile.mkdirs()) {
			tmpDir = tmpFile.getAbsolutePath();
			shutdownHook = new Thread(() -> FileUtils.deleteDirectoryQuietly(tmpFile));
			Runtime.getRuntime().addShutdownHook(shutdownHook);
		} else {
			throw new IOException("Create temp directory: " + tmpFile.getAbsolutePath() + " failed!");
		}
		for (int i = 0; i < 6; i++) {
			File distributedFile = new File(tmpDir, "file" + i);
			try (FileOutputStream out = new FileOutputStream(distributedFile)) {
				out.write(i);
			}
		}
		for (int i = 0; i < 2; i++) {
			File distributedDirectory = new File(tmpDir, "dir" + i);
			if (distributedDirectory.mkdirs()) {
				for (int j = 0; j < 2; j++) {
					File fileInDirs = new File(tmpDir, "dir" + i + File.separator + "file" + j);
					try (FileOutputStream out = new FileOutputStream(fileInDirs)) {
						out.write(i);
						out.write(j);
					}
				}
			} else {
				throw new IOException("Create temp dir: " + distributedDirectory.getAbsolutePath() + " failed!");
			}
		}
		String os = System.getProperties().getProperty("os.name").toLowerCase();
		isMacOrUnix = os.contains("mac") || os.contains("nux");
		for (int i = 0; i < 2; i++) {
			File zipFile = new File(tmpDir, "zip" + i);
			try (ZipArchiveOutputStream zipOut = new ZipArchiveOutputStream(new FileOutputStream(zipFile))) {
				ZipArchiveEntry zipfile0 = new ZipArchiveEntry("zipDir" + i + "/zipfile0");
				zipfile0.setUnixMode(0711);
				zipOut.putArchiveEntry(zipfile0);
				zipOut.write(new byte[]{1, 1, 1, 1, 1});
				zipOut.closeArchiveEntry();
				ZipArchiveEntry zipfile1 = new ZipArchiveEntry("zipDir" + i + "/zipfile1");
				zipfile1.setUnixMode(0644);
				zipOut.putArchiveEntry(zipfile1);
				zipOut.write(new byte[]{2, 2, 2, 2, 2});
				zipOut.closeArchiveEntry();
			}
			File zipExpected = new File(String.join(File.separator, tmpDir, "zipExpected" + i, "zipDir" + i));
			if (!zipExpected.mkdirs()) {
				throw new IOException("Create temp dir: " + zipExpected.getAbsolutePath() + " failed!");
			}
			File zipfile0 = new File(zipExpected, "zipfile0");
			try (FileOutputStream out = new FileOutputStream(zipfile0)) {
				out.write(new byte[]{1, 1, 1, 1, 1});
			}
			File zipfile1 = new File(zipExpected, "zipfile1");
			try (FileOutputStream out = new FileOutputStream(zipfile1)) {
				out.write(new byte[]{2, 2, 2, 2, 2});
			}
			if (isMacOrUnix) {
				if (!(zipfile0.setReadable(true, true) &&
					zipfile0.setWritable(true, true) &&
					zipfile0.setExecutable(true))) {
					throw new IOException("Set unixmode 711 to temp file: " + zipfile0.getAbsolutePath() + "failed!");
				}
				if (!(zipfile1.setReadable(true) &&
					zipfile1.setWritable(true, true) &&
					zipfile1.setExecutable(false))) {
					throw new IOException("Set unixmode 644 to temp file: " + zipfile1.getAbsolutePath() + "failed!");
				}
			}
		}
	}

	@AfterClass
	public static void cleanTempDirectory() {
		if (shutdownHook != null) {
			Runtime.getRuntime().removeShutdownHook(shutdownHook);
			shutdownHook.run();
		}
		shutdownHook = null;
		tmpDir = null;
	}

	@Test
	public void testProcessingPythonFiles() throws Exception {
		// use LinkedHashMap to preserve the path order in environment variable
		Map<String, String> filesInPythonPath = new LinkedHashMap<>();
		filesInPythonPath.put(String.join(File.separator, tmpDir, "file0"), "test_file1.py");
		filesInPythonPath.put(String.join(File.separator, tmpDir, "file1"), "test_file2.zip");
		filesInPythonPath.put(String.join(File.separator, tmpDir, "file2"), "test_file3.egg");
		filesInPythonPath.put(String.join(File.separator, tmpDir, "dir0"), "test_dir");
		PythonDependencyInfo dependencyInfo = new PythonDependencyInfo(
			filesInPythonPath, null, null, null, new HashMap<>());
		ProcessPythonEnvironmentManager environmentManager = createBasicProcessEnvironmentManager(dependencyInfo);
		environmentManager.open();
		Map<String, String> environmentVariable = environmentManager.generateEnvironmentVariables();
		environmentManager.prepareWorkingDir();

		String tmpBase = environmentManager.getBaseDirectory();
		String[] expectedUserPythonPaths = new String[] {
			String.join(File.separator, tmpBase, PYTHON_FILES_DIR, "file0"),
			String.join(File.separator, tmpBase, PYTHON_FILES_DIR, "file1", "test_file2.zip"),
			String.join(File.separator, tmpBase, PYTHON_FILES_DIR, "file2", "test_file3.egg"),
			String.join(File.separator, tmpBase, PYTHON_FILES_DIR, "dir0", "test_dir")
		};
		String expectedPythonPath = String.join(
			File.pathSeparator,
			String.join(File.pathSeparator, expectedUserPythonPaths),
			getBasicExpectedEnv(environmentManager).get("PYTHONPATH"));
		assertEquals(
			expectedPythonPath,
			environmentVariable.get("PYTHONPATH"));
		assertFileEquals(
			new File(String.join(File.separator, tmpDir, "file0")),
			new File(String.join(File.separator, tmpBase, PYTHON_FILES_DIR, "file0", "test_file1.py")));
		assertFileEquals(
			new File(String.join(File.separator, tmpDir, "file1")),
			new File(String.join(File.separator, tmpBase, PYTHON_FILES_DIR, "file1", "test_file2.zip")));
		assertFileEquals(
			new File(String.join(File.separator, tmpDir, "file2")),
			new File(String.join(File.separator, tmpBase, PYTHON_FILES_DIR, "file2", "test_file3.egg")));
		assertFileEquals(
			new File(String.join(File.separator, tmpDir, "dir0")),
			new File(String.join(File.separator, tmpBase, PYTHON_FILES_DIR, "dir0", "test_dir")));
		environmentManager.close();
	}

	@Test
	public void testProcessingPythonFilesWithCopy() throws IOException, NoSuchAlgorithmException {
		// use LinkedHashMap to preserve the path order in environment variable
		Map<String, String> filesInPythonPath = new LinkedHashMap<>();
		filesInPythonPath.put(String.join(File.separator, tmpDir, "file0"), "test_file1.py");
		filesInPythonPath.put(String.join(File.separator, tmpDir, "file1"), "test_file2.zip");
		filesInPythonPath.put(String.join(File.separator, tmpDir, "file2"), "test_file3.egg");
		filesInPythonPath.put(String.join(File.separator, tmpDir, "dir0"), "test_dir");
		String filesDirectory = String.join(File.separator, tmpDir, UUID.randomUUID().toString());

		for (Map.Entry<String, String> entry : filesInPythonPath.entrySet()) {
			// The origin file name will be wiped during the transmission in Flink Distributed Cache
			// and replaced by a unreadable character sequence, now restore their origin name.
			// Every python file will be placed at :
			// ${baseDirectory}/python_path_files_dir/${distributedCacheFileName}/${originFileName}
			String distributedCacheFileName = new File(entry.getKey()).getName();
			String actualFileName = entry.getValue();
			Path target = FileSystems.getDefault().getPath(filesDirectory,
				distributedCacheFileName, actualFileName);
			if (!target.getParent().toFile().mkdirs()) {
				throw new IllegalArgumentException(
					String.format("can not create the directory: %s !", target.getParent().toString()));
			}
			Path src = FileSystems.getDefault().getPath(entry.getKey());
			FileUtils.copy(new org.apache.flink.core.fs.Path(src.toUri()),
				new org.apache.flink.core.fs.Path(target.toUri()), false);
		}

		assertFileEquals(
			new File(String.join(File.separator, tmpDir, "file0")),
			new File(String.join(File.separator, filesDirectory, "file0", "test_file1.py")));
		assertFileEquals(
			new File(String.join(File.separator, tmpDir, "file1")),
			new File(String.join(File.separator, filesDirectory, "file1", "test_file2.zip")));
		assertFileEquals(
			new File(String.join(File.separator, tmpDir, "file2")),
			new File(String.join(File.separator, filesDirectory, "file2", "test_file3.egg")));
		assertFileEquals(
			new File(String.join(File.separator, tmpDir, "dir0")),
			new File(String.join(File.separator, filesDirectory, "dir0", "test_dir")));

		FileUtils.deleteDirectoryQuietly(new File(filesDirectory));
	}

	@Test
	public void testProcessRequirements() throws Exception {
		PythonDependencyInfo dependencyInfo = new PythonDependencyInfo(
			new HashMap<>(),
			String.join(File.separator, tmpDir, "file0"),
			String.join(File.separator, tmpDir, "dir0"),
			null, new HashMap<>());
		ProcessPythonEnvironmentManager environmentManager = createBasicProcessEnvironmentManager(dependencyInfo);
		environmentManager.open();
		Map<String, String> environmentVariable = environmentManager.generateEnvironmentVariables();
		environmentManager.prepareWorkingDir();

		String tmpBase = environmentManager.getBaseDirectory();
		Map<String, String> expected = getBasicExpectedEnv(environmentManager);
		expected.put(PYTHON_REQUIREMENTS_FILE, String.join(File.separator, tmpDir, "file0"));
		expected.put(PYTHON_REQUIREMENTS_CACHE, String.join(File.separator, tmpDir, "dir0"));
		expected.put(PYTHON_REQUIREMENTS_INSTALL_DIR, String.join(File.separator, tmpBase, PYTHON_REQUIREMENTS_DIR));
		assertEquals(expected, environmentVariable);
		environmentManager.close();
	}

	@Test
	public void testProcessArchives() throws Exception {
		// use LinkedHashMap to preserve the file order in python working directory
		Map<String, String> archives = new LinkedHashMap<>();
		archives.put(String.join(File.separator, tmpDir, "zip0"), "py27.zip");
		archives.put(String.join(File.separator, tmpDir, "zip1"), "py37");
		PythonDependencyInfo dependencyInfo = new PythonDependencyInfo(
			new HashMap<>(), null, null, null, archives);
		ProcessPythonEnvironmentManager environmentManager = createBasicProcessEnvironmentManager(dependencyInfo);
		environmentManager.open();
		Map<String, String> environmentVariable = environmentManager.generateEnvironmentVariables();
		environmentManager.prepareWorkingDir();

		String tmpBase = environmentManager.getBaseDirectory();
		Map<String, String> expected = getBasicExpectedEnv(environmentManager);
		expected.put(PYTHON_WORKING_DIR, String.join(File.separator, tmpBase, PYTHON_ARCHIVES_DIR));
		assertEquals(expected, environmentVariable);
		assertFileEquals(
			new File(String.join(File.separator, tmpDir, "zipExpected0")),
			new File(String.join(File.separator, tmpBase, PYTHON_ARCHIVES_DIR, "py27.zip")), true);
		assertFileEquals(
			new File(String.join(File.separator, tmpDir, "zipExpected1")),
			new File(String.join(File.separator, tmpBase, PYTHON_ARCHIVES_DIR, "py37")), true);
		environmentManager.close();
	}

	@Test
	public void testProcessPythonExecutable() throws Exception {
		PythonDependencyInfo dependencyInfo = new PythonDependencyInfo(
			new HashMap<>(), null, null,
			"/usr/local/bin/python",
			new HashMap<>());
		ProcessPythonEnvironmentManager environmentManager = createBasicProcessEnvironmentManager(dependencyInfo);
		environmentManager.open();
		Map<String, String> environmentVariable = environmentManager.generateEnvironmentVariables();
		environmentManager.prepareWorkingDir();

		Map<String, String> expected = getBasicExpectedEnv(environmentManager);
		expected.put("python", "/usr/local/bin/python");
		assertEquals(expected, environmentVariable);
		environmentManager.close();
	}

	@Test
	public void testCreateEnvironment() throws Exception {
		PythonDependencyInfo dependencyInfo = new PythonDependencyInfo(
			new HashMap<>(), null, null,
			null,
			new HashMap<>());
		ProcessPythonEnvironmentManager environmentManager = createBasicProcessEnvironmentManager(dependencyInfo);
		environmentManager.open();
		RunnerApi.Environment environment = environmentManager.createEnvironment();
		RunnerApi.ProcessPayload payload = RunnerApi.ProcessPayload.parseFrom(environment.getPayload());

		assertEquals(
			String.join(File.separator, environmentManager.getBaseDirectory(), "pyflink-udf-runner.sh"),
			payload.getCommand());
		Map<String, String> expectedEnv = getBasicExpectedEnv(environmentManager);
		assertEquals(expectedEnv, payload.getEnvMap());
		environmentManager.close();
	}

	@Test
	public void testCreateRetrievalToken() throws Exception {
		PythonDependencyInfo dependencyInfo = new PythonDependencyInfo(
			new HashMap<>(), null, null,
			null,
			new HashMap<>());
		Map<String, String> sysEnv = new HashMap<>();
		sysEnv.put("FLINK_HOME", "/flink");
		ProcessPythonEnvironmentManager environmentManager =
			new ProcessPythonEnvironmentManager(dependencyInfo, new String[] {tmpDir}, null, sysEnv);
		environmentManager.open();
		String retrivalToken = environmentManager.createRetrievalToken();

		File retrivalTokenFile = new File(retrivalToken);
		byte[] content = new byte[(int) retrivalTokenFile.length()];
		try (DataInputStream input = new DataInputStream(new FileInputStream(retrivalToken))) {
			input.readFully(content);
		}
		assertEquals("{\"manifest\": {}}", new String(content));
		environmentManager.close();
	}

	@Test
	public void testSetLogDirectory() throws Exception {
		PythonDependencyInfo dependencyInfo = new PythonDependencyInfo(
			new HashMap<>(), null, null,
			null,
			new HashMap<>());
		ProcessPythonEnvironmentManager environmentManager = new ProcessPythonEnvironmentManager(
			dependencyInfo, new String[] {tmpDir}, "/tmp/log", new HashMap<>());
		environmentManager.open();
		Map<String, String> env = environmentManager.generateEnvironmentVariables();
		Map<String, String> expected = getBasicExpectedEnv(environmentManager);
		expected.put("FLINK_LOG_DIR", "/tmp/log");
		assertEquals(expected, env);
		environmentManager.close();
	}

	@Test
	public void testOpenClose() throws Exception {
		PythonDependencyInfo dependencyInfo = new PythonDependencyInfo(
			new HashMap<>(), null, null,
			null,
			new HashMap<>());
		ProcessPythonEnvironmentManager environmentManager = createBasicProcessEnvironmentManager(dependencyInfo);
		environmentManager.open();
		environmentManager.createRetrievalToken();

		String tmpBase = environmentManager.getBaseDirectory();
		assertTrue(new File(tmpBase).isDirectory());
		environmentManager.close();
		assertFalse(new File(tmpBase).exists());
	}

	private static void assertFileEquals(File expectedFile, File actualFile)
		throws IOException, NoSuchAlgorithmException {
		assertFileEquals(expectedFile, actualFile, false);
	}

	private static void assertFileEquals(File expectedFile, File actualFile, boolean checkUnixMode)
		throws IOException, NoSuchAlgorithmException {
		assertTrue(actualFile.exists());
		assertTrue(expectedFile.exists());
		if (expectedFile.getAbsolutePath().equals(actualFile.getAbsolutePath())) {
			return;
		}
		if (isMacOrUnix && checkUnixMode) {
			Set<PosixFilePermission> expectedPerm = Files.getPosixFilePermissions(Paths.get(expectedFile.toURI()));
			Set<PosixFilePermission> actualPerm = Files.getPosixFilePermissions(Paths.get(actualFile.toURI()));
			assertEquals(expectedPerm, actualPerm);
		}
		if (expectedFile.isDirectory()) {
			assertTrue(actualFile.isDirectory());
			String[] expectedSubFiles = expectedFile.list();
			assertArrayEquals(expectedSubFiles, actualFile.list());
			if (expectedSubFiles != null) {
				for (String fileName : expectedSubFiles) {
					assertFileEquals(
						new File(expectedFile.getAbsolutePath(), fileName),
						new File(actualFile.getAbsolutePath(), fileName));
				}
			}
		} else {
			assertEquals(expectedFile.length(), actualFile.length());
			if (expectedFile.length() > 0) {
				assertEquals(computeSHA1Checksum(expectedFile), computeSHA1Checksum(actualFile));
			}
		}
	}

	private static String computeSHA1Checksum(File file) throws IOException, NoSuchAlgorithmException {
		try (InputStream fis =  new FileInputStream(file)) {
			byte[] buffer = new byte[1024];
			MessageDigest complete = MessageDigest.getInstance("SHA1");
			int numRead;
			do {
				numRead = fis.read(buffer);
				if (numRead > 0) {
					complete.update(buffer, 0, numRead);
				}
			} while (numRead != -1);
			return new BigInteger(1, complete.digest()).toString(16);
		}
	}

	private static Map<String, String> getBasicExpectedEnv(ProcessPythonEnvironmentManager environmentManager) {
		Map<String, String> map = new HashMap<>();
		String tmpBase = environmentManager.getBaseDirectory();
		map.put(
			"PYTHONPATH",
			String.join(
				File.pathSeparator,
				String.join(File.separator, tmpBase, "pyflink.zip"),
				String.join(File.separator, tmpBase, "py4j-0.10.8.1-src.zip"),
				String.join(File.separator, tmpBase, "cloudpickle-1.2.2-src.zip")));
		return map;
	}

	private static ProcessPythonEnvironmentManager createBasicProcessEnvironmentManager(
			PythonDependencyInfo dependencyInfo) {
		return new ProcessPythonEnvironmentManager(
			dependencyInfo, new String[] {tmpDir}, null, new HashMap<>());
	}
}
