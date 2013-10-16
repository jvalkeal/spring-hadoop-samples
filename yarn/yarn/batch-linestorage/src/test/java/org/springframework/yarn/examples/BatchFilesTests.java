/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.yarn.examples;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.junit.Test;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.test.annotation.Timed;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.yarn.test.context.MiniYarnCluster;
import org.springframework.yarn.test.context.YarnDelegatingSmartContextLoader;
import org.springframework.yarn.test.junit.AbstractYarnClusterTests;

/**
 * Tests for batch files example.
 *
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration(loader=YarnDelegatingSmartContextLoader.class)
@MiniYarnCluster
public class BatchFilesTests extends AbstractYarnClusterTests {

	@Test
	@Timed(millis=300000)
	public void testAppSubmission() throws Exception {
		createTestData("data1.txt", "set1", 300, "line1");
		createTestData("data2.txt", "set1", 300, "line2");
		createTestData("data3.txt", "set1", 300, "line3");

		YarnApplicationState state = submitApplicationAndWait(240, TimeUnit.SECONDS);
		assertNotNull(state);
		assertTrue(state.equals(YarnApplicationState.FINISHED));

		File workDir = getYarnCluster().getYarnWorkDir();

		PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
		String locationPattern = "file:" + workDir.getAbsolutePath() + "/**/*.std*";
		Resource[] resources = resolver.getResources(locationPattern);

		assertThat(resources, notNullValue());
		assertThat(resources.length, is(8));

		int linesFound = 0;
		HashSet<String> linesUnique = new HashSet<String>(300);

		for (Resource res : resources) {
			File file = res.getFile();
			if (file.getName().endsWith("stdout")) {
				// there has to be some content in stdout file
				assertThat(file.length(), greaterThan(0l));
				if (file.getName().equals("Container.stdout")) {
					Scanner scanner = new Scanner(file);
					while (scanner.hasNextLine()) {
						String line = scanner.nextLine();
						if (line.contains("writing:")) {
							String[] split = line.split("\\s+");
							linesUnique.add(split[split.length-1]);
							linesFound++;
						}
					}
					scanner.close();
				}
			} else if (file.getName().endsWith("stderr")) {
				// can't have anything in stderr files
				assertThat(file.length(), is(0l));
			}
		}

		assertThat(linesFound, is(900));
		assertThat(linesUnique.size(), is(900));
	}

	private void createTestData(String fileName, String dir, int count, String prefix) throws IOException {
		FileSystem fs = FileSystem.get(getYarnCluster().getConfiguration());
		Path path = new Path("/syarn-tmp/batch-linestorage/" + dir + "/" + fileName);
		FSDataOutputStream out = fs.create(path);
		for (int i = 0; i<count; i++) {
			out.writeBytes(prefix + i + "\n");
		}
		out.close();
		assertTrue(fs.exists(path));
		assertThat(fs.getFileStatus(path).getLen(), greaterThan(0l));
	}

}
