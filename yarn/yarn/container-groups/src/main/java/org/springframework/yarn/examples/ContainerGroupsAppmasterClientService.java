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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.integration.ip.mind.MindAppmasterService;
import org.springframework.yarn.integration.ip.mind.MindRpcMessageHolder;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

/**
 * Custom application master service handling communication
 * from a client. For simplicity expects message exchange
 * to happen using {@link GroupResizeRequest} and {@link GroupResizeResponse}.
 * <p>
 * Communicates with application master to pass incoming commands.
 * Application master is autowired into this instance.
 *
 * @author Janne Valkealahti
 *
 */
public class ContainerGroupsAppmasterClientService extends MindAppmasterService {

	private static final Log log = LogFactory.getLog(ContainerGroupsAppmasterClientService.class);

	@Autowired
	private ContainerGroupsAppmaster customAppmaster;

	@Override
	protected MindRpcMessageHolder handleMindMessageInternal(MindRpcMessageHolder message) {
		BaseObject request = getConversionService().convert(message, BaseObject.class);
		GroupResizeResponse response = handleJob((GroupResizeRequest)request);
		return getConversionService().convert(response, MindRpcMessageHolder.class);
	}

	private GroupResizeResponse handleJob(GroupResizeRequest request) {
		log.info("XXX Request: group=" + request.getGroup() + " size=" + request.getSize());
		GroupResizeResponse response = new GroupResizeResponse();

		customAppmaster.setGroupSize(request.getGroup(), request.getSize());
		response.setState(GroupResizeResponse.State.OK);
		log.info("XXX Response: state=" + response.getState());

		return response;
	}


}
