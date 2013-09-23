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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.config.annotation.EnableYarn;
import org.springframework.yarn.config.annotation.EnableYarn.Enable;
import org.springframework.yarn.config.annotation.SpringYarnConfigurerAdapter;
import org.springframework.yarn.config.annotation.builders.YarnAppmasterConfigure;
import org.springframework.yarn.config.annotation.builders.YarnConfigConfigure;
import org.springframework.yarn.config.annotation.builders.YarnEnvironmentConfigure;
import org.springframework.yarn.config.annotation.builders.YarnResourceLocalizerConfigure;

@Configuration
@EnableYarn(enable=Enable.APPMASTER)
public class AppmasterConfiguration extends SpringYarnConfigurerAdapter {

	@Autowired
	private Environment env;

	@Override
	public void configure(YarnConfigConfigure config) throws Exception {
		config
			.withProperties()
				.property("fs.defaultFS", env.getProperty(YarnSystemConstants.FS_ADDRESS))
				.property("yarn.resourcemanager.address", env.getProperty(YarnSystemConstants.RM_ADDRESS))
				.property("yarn.resourcemanager.scheduler.address", env.getProperty(YarnSystemConstants.SCHEDULER_ADDRESS));
	}

	@Override
	public void configure(YarnResourceLocalizerConfigure localizer) throws Exception {
		localizer
			.withHdfs()
				.hdfs("/app/simple-command/*.jar")
				.hdfs("/lib/*.jar");
	}

	@Override
	public void configure(YarnEnvironmentConfigure environment) throws Exception {
		environment
			.includeSystemEnv(true)
			.withClasspath()
				.entry("./*")
				.defaultYarnAppClasspath(true);
	}

	@Override
	public void configure(YarnAppmasterConfigure master) throws Exception {
		master
			.setCommands(new String[]{"date","1><LOG_DIR>/Container.stdout","2><LOG_DIR>/Container.stderr"});
	}

}
