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

import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;

/**
 * State response for group sizing request.
 *
 * @author Janne Valkealahti
 *
 */
public class GroupResizeResponse extends BaseResponseObject {

	public State state;

	public GroupResizeResponse() {
		super();
	}

	public GroupResizeResponse(State state) {
		super();
		this.state = state;
	}

	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}

	public enum State {
		OK,
		ERROR
	}

}
