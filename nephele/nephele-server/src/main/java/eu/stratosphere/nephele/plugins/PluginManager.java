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

package eu.stratosphere.nephele.plugins;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.protocols.PluginCommunicationProtocol;
import eu.stratosphere.nephele.taskmanager.TaskManager;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * The plugin manager is responsible for loading and managing the individual
 * plugins.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class PluginManager {

	/**
	 * The log object used to report errors and information in general.
	 */
	private static final Log LOG = LogFactory.getLog(PluginManager.class);

	private static final String PLUGINS_NAMESPACE_KEY_PREFIX = "plugins.";

	/**
	 * Pattern for plugins classname property.
	 */
	private static final String PLUGINS_CLASSNAME_KEY_PATTERN = PLUGINS_NAMESPACE_KEY_PREFIX
			+ "%s.classname";

	/**
	 * The singleton instance of this class.
	 */
	private static PluginManager INSTANCE = null;

	private final Map<String, AbstractPluginLoader> plugins;

	private PluginManager(final PluginLookupService pluginLookupService) {
		this.plugins = getPluginLoaders(pluginLookupService);
	}

	private Map<String, AbstractPluginLoader> getPluginLoaders(
			final PluginLookupService pluginLookupService) {

		final Map<String, AbstractPluginLoader> tmpPluginList = new LinkedHashMap<String, AbstractPluginLoader>();

		for (String pluginName : getPluginNames()) {
			AbstractPluginLoader pluginLoader = getPluginLoader(pluginName,
					pluginLookupService);

			if (pluginLoader != null) {
				tmpPluginList.put(pluginName, pluginLoader);
			}
		}

		return Collections.unmodifiableMap(tmpPluginList);
	}

	private Set<String> getPluginNames() {
		HashSet<String> pluginNames = new HashSet<String>();

		for (String key : GlobalConfiguration.getConfiguration().keySet()) {
			if (key.startsWith(PLUGINS_NAMESPACE_KEY_PREFIX)) {

				String[] splits = key.substring(
						PLUGINS_NAMESPACE_KEY_PREFIX.length()).split("\\.");
				if (splits.length >= 1) {
					pluginNames.add(splits[0]);
				} else {
					LOG.warn("Invalid key in Nephele plugins namespace: " + key);
				}
			}
		}
		return pluginNames;
	}

	private static synchronized PluginManager getInstance(
			final PluginLookupService pluginLookupService) {

		if (INSTANCE == null) {
			INSTANCE = new PluginManager(pluginLookupService);
		}

		return INSTANCE;
	}

	private Map<PluginID, JobManagerPlugin> getJobManagerPluginsInternal() {

		final Map<PluginID, JobManagerPlugin> jobManagerPluginMap = new HashMap<PluginID, JobManagerPlugin>();

		final Iterator<AbstractPluginLoader> it = this.plugins.values()
				.iterator();
		while (it.hasNext()) {

			final AbstractPluginLoader apl = it.next();
			final PluginID pluginID = apl.getPluginID();
			final JobManagerPlugin jmp = apl.getJobManagerPlugin();
			if (jmp != null) {
				if (!jobManagerPluginMap.containsKey(pluginID)) {
					jobManagerPluginMap.put(pluginID, jmp);
				} else {
					LOG.error("Detected ID collision for plugin "
							+ apl.getPluginName() + ", skipping it...");
				}
			}
		}

		return Collections.unmodifiableMap(jobManagerPluginMap);
	}

	private Map<PluginID, TaskManagerPlugin> getTaskManagerPluginsInternal() {

		final Map<PluginID, TaskManagerPlugin> taskManagerPluginMap = new HashMap<PluginID, TaskManagerPlugin>();

		final Iterator<AbstractPluginLoader> it = this.plugins.values()
				.iterator();
		while (it.hasNext()) {

			final AbstractPluginLoader apl = it.next();
			final PluginID pluginID = apl.getPluginID();
			final TaskManagerPlugin tmp = apl.getTaskManagerPlugin();
			if (tmp != null) {
				if (!taskManagerPluginMap.containsKey(pluginID)) {
					taskManagerPluginMap.put(pluginID, tmp);
				} else {
					LOG.error("Detected ID collision for plugin "
							+ apl.getPluginName() + ", skipping it...");
				}
			}
		}

		return Collections.unmodifiableMap(taskManagerPluginMap);
	}

	@SuppressWarnings("unchecked")
	private static AbstractPluginLoader getPluginLoader(String pluginName,
			PluginLookupService pluginLookupService) {

		String pluginClassnameKey = String.format(
				PLUGINS_CLASSNAME_KEY_PATTERN, pluginName);
		String pluginClassname = GlobalConfiguration.getString(
				pluginClassnameKey, null);

		if (pluginClassname == null) {
			LOG.error(String.format(
					"Unable to load plugin %s: Property %s is missing",
					pluginName, pluginClassnameKey));
			return null;
		}

		AbstractPluginLoader pluginLoader = null;

		try {
			Class<? extends AbstractPluginLoader> loaderClass = (Class<? extends AbstractPluginLoader>) Class
					.forName(pluginClassname);
			Constructor<? extends AbstractPluginLoader> constructor = (Constructor<? extends AbstractPluginLoader>) loaderClass
					.getConstructor(String.class, PluginLookupService.class);
			pluginLoader = constructor.newInstance(pluginName,
					pluginLookupService);
			LOG.info("Successfully loaded plugin " + pluginName);
		} catch (Exception e) {
			LOG.error("Unable to load plugin " + pluginName + ": "
					+ StringUtils.stringifyException(e));
		}

		return pluginLoader;
	}

	public static Map<PluginID, JobManagerPlugin> getJobManagerPlugins(
			final PluginCommunicationProtocol jobManager) {

		final JobManagerLookupService lookupService = new JobManagerLookupService(
				jobManager);

		return getInstance(lookupService).getJobManagerPluginsInternal();
	}

	public static Map<PluginID, TaskManagerPlugin> getTaskManagerPlugins(
			final TaskManager taskManager) {

		final TaskManagerLookupService lookupService = new TaskManagerLookupService(
				taskManager);

		return getInstance(lookupService).getTaskManagerPluginsInternal();
	}
	
	public static String prefixWithPluginNamespace(String keySuffix) {
		return PLUGINS_NAMESPACE_KEY_PREFIX + keySuffix;
	}
}
