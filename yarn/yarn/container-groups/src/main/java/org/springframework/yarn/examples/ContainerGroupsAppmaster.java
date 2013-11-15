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
import org.springframework.data.grid.listener.ContainerGridGroupsListener;
import org.springframework.data.grid.listener.ContainerGridListener;
import org.springframework.yarn.am.AbstractManagedContainerGroupsAppmaster;
import org.springframework.yarn.grid.YarnContainerGroup;
import org.springframework.yarn.grid.YarnContainerNode;
import org.springframework.yarn.grid.YarnManagedContainerGridGroups;

/**
 * Custom appmaster using managed container groups concept.
 *
 * @author Janne Valkealahti
 *
 */
public class ContainerGroupsAppmaster extends AbstractManagedContainerGroupsAppmaster {

	private final static Log log = LogFactory.getLog(ContainerGroupsAppmaster.class);

	@Autowired
	private YarnManagedContainerGridGroups managedGroups;

	public void setGroupSize(String group, int size) {
		log.info("XXX Request for group " + group + " for size " + size);
		managedGroups.setGroupSize(group, size);
	}

	@Override
	protected void onInit() throws Exception {
		setManagedGroups(managedGroups);

		managedGroups.addContainerGridListener(new ContainerGridListener<String, YarnContainerNode>() {
			@Override
			public void nodeRemoved(YarnContainerNode node) {
				log.info("XXX node removed from grid: " + node);
			}

			@Override
			public void nodeAdded(YarnContainerNode node) {
				log.info("XXX node added to grid: " + node);
			}
		});

		managedGroups.addContainerGridGroupsListener(new ContainerGridGroupsListener<String, String, YarnContainerNode, YarnContainerGroup>() {
			@Override
			public void nodeRemoved(YarnContainerGroup group, YarnContainerNode node) {
				log.info("XXX node removed from group: " + group + " " + node);
			}

			@Override
			public void nodeAdded(YarnContainerGroup group, YarnContainerNode node) {
				log.info("XXX node added to group: " + group + " " + node);
			}

			@Override
			public void groupRemoved(YarnContainerGroup group) {
				log.info("XXX group removed from grid: " + group);
			}

			@Override
			public void groupAdded(YarnContainerGroup group) {
				log.info("XXX group added to grid: " + group);
			}
		});

		super.onInit();
	}

}
