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

import java.io.File;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.junit.Test;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.core.io.Resource;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.ConsumerEndpointFactoryBean;
import org.springframework.integration.ip.tcp.TcpOutboundGateway;
import org.springframework.integration.ip.tcp.connection.TcpNetClientConnectionFactory;
import org.springframework.test.annotation.Timed;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.yarn.am.AppmasterServiceClient;
import org.springframework.yarn.integration.IntegrationAppmasterServiceClientFactoryBean;
import org.springframework.yarn.integration.ip.mind.DefaultMindAppmasterServiceClient;
import org.springframework.yarn.integration.ip.mind.MindAppmasterServiceClient;
import org.springframework.yarn.integration.ip.mind.MindRpcSerializer;
import org.springframework.yarn.test.context.MiniYarnCluster;
import org.springframework.yarn.test.context.YarnDelegatingSmartContextLoader;
import org.springframework.yarn.test.junit.AbstractYarnClusterTests;
import org.springframework.yarn.test.junit.ApplicationInfo;
import org.springframework.yarn.test.support.ContainerLogUtils;

/**
 * Tests for container groups example.
 *
 * @author Janne Valkealahti
 *
 */
@ContextConfiguration(loader=YarnDelegatingSmartContextLoader.class)
@MiniYarnCluster
public class ContainerGroupsTests extends AbstractYarnClusterTests implements BeanFactoryAware {

	private final static Log log = LogFactory.getLog(ContainerGroupsTests.class);

	private BeanFactory beanFactory;

	@Test
	@Timed(millis=200000)
	public void testAppSubmission() throws Exception {

		// submit, there should be no running containers
		ApplicationInfo info = submitApplicationAndWaitState(120, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
		assertThat(info, notNullValue());
		assertThat(info.getYarnApplicationState(), notNullValue());
		assertThat(info.getApplicationId(), notNullValue());

		// need to get appmaster rpc host and port
		ApplicationReport report = getYarnClient().getApplicationReport(info.getApplicationId());

		// ramp up testgroup with 1 container
		MindAppmasterServiceClient client = (MindAppmasterServiceClient) createClient(report.getHost(), report.getRpcPort());
		GroupResizeRequest request = new GroupResizeRequest("testgroup", 1);
		GroupResizeResponse response = (GroupResizeResponse) client.doMindRequest(request);
		Thread.sleep(30000);

		request.setSize(0);
		response = (GroupResizeResponse) client.doMindRequest(request);

		Thread.sleep(20000);
		YarnApplicationState waitState = waitState(info.getApplicationId(), 60, TimeUnit.SECONDS, YarnApplicationState.FINISHED, YarnApplicationState.FAILED);

		// ramp up testgroup with 1 container, now 2

		// ramp down testgroup with 1 container, now 1

		// ramp down testgroup with 1 container, now 0

		getYarnClient().killApplication(info.getApplicationId());
		waitState = waitState(info.getApplicationId(), 30, TimeUnit.SECONDS, YarnApplicationState.FAILED, YarnApplicationState.FINISHED, YarnApplicationState.KILLED);

		// check and assert logs
		List<Resource> resources = ContainerLogUtils.queryContainerLogs(getYarnCluster(), info.getApplicationId());
		assertThat(resources, notNullValue());
		assertThat(resources.size(), greaterThan(2));

		for (Resource res : resources) {
			File file = res.getFile();
			if (file.getName().endsWith("stderr")) {
				String content = "";
				if (file.length() > 0) {
					Scanner scanner = new Scanner(file);
					content = scanner.useDelimiter("\\A").next();
					scanner.close();
				}
				// can't have anything in stderr files
				assertThat("stderr file is not empty: " + content, file.length(), is(0l));
			}
		}
	}

	private AppmasterServiceClient createClient(String host, int port) throws Exception {
		// TODO: uggly, ok for testing, should make this more generic
		IntegrationAppmasterServiceClientFactoryBean factory = new IntegrationAppmasterServiceClientFactoryBean();

		DirectChannel reqChannel = new DirectChannel();
		QueueChannel resChannel = new QueueChannel();

		MindRpcSerializer serializer = new MindRpcSerializer();

		TcpNetClientConnectionFactory connFactory = new TcpNetClientConnectionFactory(host, port);
		connFactory.setSerializer(serializer);
		connFactory.setDeserializer(serializer);
		connFactory.start();

		TcpOutboundGateway gateway = new TcpOutboundGateway();
		gateway.setConnectionFactory(connFactory);
		gateway.setOutputChannel(reqChannel);
		gateway.setReplyChannel(resChannel);
		gateway.setRequiresReply(true);
		gateway.setBeanFactory(beanFactory);
		gateway.afterPropertiesSet();
		gateway.start();

		ConsumerEndpointFactoryBean endpointFactory = new ConsumerEndpointFactoryBean();
		endpointFactory.setBeanFactory(beanFactory);
		endpointFactory.setHandler(gateway);
		endpointFactory.setInputChannel(reqChannel);
		endpointFactory.afterPropertiesSet();
		endpointFactory.start();
		endpointFactory.getObject();

		factory.setServiceImpl(DefaultMindAppmasterServiceClient.class);
		factory.setBeanFactory(beanFactory);
		factory.setRequestChannel(reqChannel);
		factory.setResponseChannel(resChannel);
		factory.afterPropertiesSet();
		return factory.getObject();
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

}
