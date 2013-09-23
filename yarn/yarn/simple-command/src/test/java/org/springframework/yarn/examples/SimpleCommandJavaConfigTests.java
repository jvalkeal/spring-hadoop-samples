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

import java.util.Properties;

import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.yarn.config.annotation.EnableYarn;
import org.springframework.yarn.config.annotation.EnableYarn.Enable;
import org.springframework.yarn.config.annotation.SpringYarnConfigurerAdapter;
import org.springframework.yarn.config.annotation.builders.YarnClientConfigure;
import org.springframework.yarn.config.annotation.builders.YarnEnvironmentConfigure;
import org.springframework.yarn.config.annotation.builders.YarnResourceLocalizerConfigure;
import org.springframework.yarn.test.context.MiniYarnCluster;
import org.springframework.yarn.test.context.YarnDelegatingSmartContextLoader;

/**
 * Use java config.
 *
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration(loader=YarnDelegatingSmartContextLoader.class)
@MiniYarnCluster
public class SimpleCommandJavaConfigTests extends SimpleCommandTests {

	@Configuration
	@EnableYarn(enable=Enable.CLIENT)
	static class ClientConfiguration extends SpringYarnConfigurerAdapter {

		@Override
		public void configure(YarnResourceLocalizerConfigure localizer) throws Exception {
			localizer
				.withCopy()
					.copy("file:build/dependency-libs/*", "/lib/", false)
					.copy("file:build/libs/*", "/app/simple-command/", false)
					.and()
				.withHdfs()
					.hdfs("/app/simple-command/*.jar")
					.hdfs("/lib/*.jar");
		}

		@Override
		public void configure(YarnEnvironmentConfigure environment) throws Exception {
			environment
				.withClasspath()
					.entry("./*")
					.defaultYarnAppClasspath(true);
		}

		@Override
		public void configure(YarnClientConfigure client) throws Exception {
			Properties arguments = new Properties();
			arguments.put("container-count", "4");
			client
				.withMasterRunner()
					.contextClass(AppmasterConfiguration.class)
					.arguments(arguments);
		}

	}

}
