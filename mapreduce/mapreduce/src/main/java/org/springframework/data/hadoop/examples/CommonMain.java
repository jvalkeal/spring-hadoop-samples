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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.mapreduce.JobRunner;

/**
 * Main class for mapreduce examples.
 *
 * @author Janne Valkealahti
 *
 */
public class CommonMain {

	private static final Log log = LogFactory.getLog(CommonMain.class);

	public static void main(String args[]) {

		ConfigurableApplicationContext context = null;

		try {
			System.out.println("Running example");
			context = new ClassPathXmlApplicationContext("application-context.xml");			
			JobRunner runner = (JobRunner) context.getBean("runner");
			runner.call();
			System.out.println("Complete example");
		} catch (Throwable e) {
			log.error("Error in main method", e);
		} finally {
			if (context != null) {
				context.close();
			}
		}

	}

}
