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
package org.springframework.data.hadoop.examples;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.springframework.data.hadoop.mapreduce.JobRunner;
import org.springframework.data.hadoop.test.context.HadoopDelegatingSmartContextLoader;
import org.springframework.data.hadoop.test.context.MiniHadoopCluster;
import org.springframework.data.hadoop.test.junit.AbstractMapReduceTests;
import org.springframework.test.context.ContextConfiguration;

/**
 * Tests for wordcount example.
 * 
 * @author Janne Valkealahti
 * 
 */
@ContextConfiguration(loader=HadoopDelegatingSmartContextLoader.class)
@MiniHadoopCluster
public class WordcountTests extends AbstractMapReduceTests {
	
	@Test
	public void testWordcountJob() throws Exception {

		// run blocks and throws exception if job failed
		JobRunner runner = (JobRunner) getApplicationContext().getBean("runner");
		runner.call();

		// get output files from a job
		Path[] outputFiles = getOutputFilePaths("/user/gutenberg/output/word/");
		assertEquals(1, outputFiles.length);
		assertThat(getFileSystem().getFileStatus(outputFiles[0]).getLen(), greaterThan(0l));

		// read through the file and check that line with
		// "themselves	6" was found
		boolean found = false;
		InputStream in = getFileSystem().open(outputFiles[0]);
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String line = null;
		while ((line = reader.readLine()) != null) {
			if (line.startsWith("themselves")) {
				assertThat(line, is("themselves\t6"));
				found = true;
			}
		}
		reader.close();
		assertThat("Keyword 'themselves' not found", found);
	}

}
