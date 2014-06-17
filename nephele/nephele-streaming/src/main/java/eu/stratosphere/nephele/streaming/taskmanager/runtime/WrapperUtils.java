/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.streaming.taskmanager.runtime;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * This class contains convenience methods to access wrapped Nephele task
 * classes.
 * 
 * @author warneke
 */
public final class WrapperUtils {

	/**
	 * The configuration key to access the name of the wrapped class from the
	 * task configuration.
	 */
	public static final String WRAPPED_CLASS_KEY = "streaming.class.name";

	/**
	 * Private constructor so class cannot be instantiated.
	 */
	private WrapperUtils() {
	}

	public static void wrapOutputClass(JobOutputVertex vertex) {
		vertex.getConfiguration().setString(WrapperUtils.WRAPPED_CLASS_KEY,
				vertex.getOutputClass().getName());
		vertex.setOutputClass(StreamOutputTaskWrapper.class);
	}

	public static void wrapInputClass(JobInputVertex vertex) {
		vertex.getConfiguration().setString(WrapperUtils.WRAPPED_CLASS_KEY,
				vertex.getInputClass().getName());
		vertex.setInputClass(StreamInputTaskWrapper.class);
	}

	public static void wrapTaskClass(JobTaskVertex vertex) {
		vertex.getConfiguration().setString(WrapperUtils.WRAPPED_CLASS_KEY,
				vertex.getTaskClass().getName());
		vertex.setTaskClass(StreamTaskWrapper.class);
	}

	/**
	 * Retrieves the name of the original class from the task configuration,
	 * loads the class, creates an instances of it and sets its environment.
	 * 
	 * @param environment
	 *            the environment to set on the new invokable
	 * @return an instance of the wrapped invokable class
	 */
	public static AbstractInvokable getWrappedInvokable(
			final StreamTaskEnvironment environment) {

		AbstractInvokable wrappedInvokable = null;

		final Configuration taskConfiguration = environment
				.getTaskConfiguration();
		final JobID jobID = environment.getJobID();
		final String className = taskConfiguration.getString(WRAPPED_CLASS_KEY,
				null);
		if (className == null) {
			throw new IllegalStateException("Cannot find name of wrapped class");
		}

		try {
			final ClassLoader cl = LibraryCacheManager.getClassLoader(jobID);

			@SuppressWarnings("unchecked")
			final Class<? extends AbstractInvokable> invokableClass = (Class<? extends AbstractInvokable>) Class
					.forName(className, true, cl);

			wrappedInvokable = invokableClass.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(StringUtils.stringifyException(e));
		}

		wrappedInvokable.setEnvironment(environment);

		return wrappedInvokable;
	}

	public static StreamTaskEnvironment getWrappedEnvironment(
			RuntimeEnvironment environment) {
		return new StreamTaskEnvironment(environment);
	}

}
