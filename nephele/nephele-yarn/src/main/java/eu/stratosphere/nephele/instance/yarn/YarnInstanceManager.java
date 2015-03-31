/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.instance.yarn;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.*;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.taskmanager.TaskManager;
import eu.stratosphere.nephele.topology.NetworkTopology;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public final class YarnInstanceManager implements InstanceManager {

	/**
	 * The log object used to report debugging and error information.
	 */
	private static final Log LOG = LogFactory.getLog(YarnInstanceManager.class);

	private final AMRMClientAsync amRmClient;

	private final NMClientAsync nmClient;

	/**
	 * The default instance type.
	 */
	private final InstanceType defaultInstanceType = InstanceTypeFactory.constructFromDescription(ConfigConstants.DEFAULT_INSTANCE_TYPE);

	private InstanceTypeDescription defaultInstanceTypeDescription = null;

	private final Map<ContainerId, Container> allocatedContainers = new HashMap<ContainerId, Container>();

	/**
	 * Set of hosts known to run a task manager that are thus able to execute
	 * tasks.
	 */
	private final Map<InstanceConnectionInfo, YarnInstance> registeredInstances = new HashMap<InstanceConnectionInfo, YarnInstance>();

	private final Map<AllocationID, AllocatedResource> allocatedResources = new HashMap<AllocationID, AllocatedResource>();


	/**
	 * Object that is notified if instances become available or vanish.
	 */
	private InstanceListener instanceListener;


	private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

		@Override
		public synchronized void onContainersCompleted(List<ContainerStatus> containerStatuses) {

		}

		@Override
		public synchronized void onContainersAllocated(List<Container> containers) {
			LOG.info("Got response from RM for container ask, allocatedCnt=" + containers.size());

			for (Container container : containers) {
				allocatedContainers.put(container.getId(), container);

				// Set up the container launch context for the application master
				List<String> commands = new ArrayList<String>();
				commands.add("/home/bjoern/stratosphere/bin/nephele-taskmanager.sh start");

				HashMap<String, String> env = new HashMap<String, String>();
				InetSocketAddress rpcAddress = JobManager.getInstance().getRpcServerAddress();
				env.put(TaskManager.ENV_YARN_MODE_NEPHELE_JM_RPC_ADDRESS, rpcAddress.getHostString());
				env.put(TaskManager.ENV_YARN_MODE_NEPHELE_JM_RPC_PORT, Integer.toString(rpcAddress.getPort()));

				ContainerLaunchContext launchContext = ContainerLaunchContext.newInstance(
								new HashMap<String, LocalResource>(), env, commands, null, null, null);

				nmClient.startContainerAsync(container, launchContext);
			}
		}

		@Override
		public void onShutdownRequest() {

		}

		@Override
		public void onNodesUpdated(List<NodeReport> nodeReports) {

		}

		@Override
		public float getProgress() {
			return 0;
		}

		@Override
		public void onError(Throwable throwable) {

		}
	}

	private class NMCallbackHandler implements NMClientAsync.CallbackHandler {

		@Override
		public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> stringByteBufferMap) {

		}

		@Override
		public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {

		}

		@Override
		public void onContainerStopped(ContainerId containerId) {

		}

		@Override
		public void onStartContainerError(ContainerId containerId, Throwable throwable) {

		}

		@Override
		public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {

		}

		@Override
		public void onStopContainerError(ContainerId containerId, Throwable throwable) {

		}
	}


	public YarnInstanceManager() throws Exception {
		String containerIdString = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
		if(containerIdString == null) {
			throw new Exception("Environment variable missing: " + ApplicationConstants.Environment.CONTAINER_ID.name());
		}

		String rmAdressStr = System.getenv("NEPHELE_YARN_AM_RM_HOSTNAME");
		if(rmAdressStr == null) {
			throw new Exception("Environment variable missing: NEPHELE_YARN_AM_RM_HOSTNAME");
		}

		ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
		ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();

		YarnConfiguration amrmConf = new YarnConfiguration();
		amrmConf.set(YarnConfiguration.RM_HOSTNAME, rmAdressStr);

		AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
		amRmClient = AMRMClientAsync.createAMRMClientAsync(500, allocListener);
		amRmClient.init(amrmConf);
		amRmClient.start();

		YarnConfiguration amnmConf = new YarnConfiguration();
		amnmConf.set(YarnConfiguration.NM_ADDRESS,
						String.format("%s:%s",
										System.getenv(ApplicationConstants.Environment.NM_HOST.name()),
										System.getenv(ApplicationConstants.Environment.NM_PORT.name())));
		nmClient = NMClientAsync.createNMClientAsync(new NMCallbackHandler());
		nmClient.init(amnmConf);
		nmClient.start();

		// Register self with ResourceManager
		// This will start heartbeating to the RM
		String appMasterHostname = NetUtils.getHostname();
		RegisterApplicationMasterResponse response = amRmClient.registerApplicationMaster(appMasterHostname,
						GlobalConfiguration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT),
						String.format("http://%s:%d", appMasterHostname,
										GlobalConfiguration.getInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, ConfigConstants.DEFAULT_WEB_FRONTEND_PORT)));

		// Dump out information about cluster capability as seen by the
		// resource manager
		int maxMem = response.getMaximumResourceCapability().getMemory();
		LOG.info("Max mem capability of resources in this cluster " + maxMem);

		int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
		LOG.info("Max vcores capability of resources in this cluster " + maxVCores);
	}

	@Override
	public void requestInstance(JobID jobID, Configuration conf, InstanceRequestMap instanceRequestMap, List<String> splitAffinityList) throws InstanceException {
		Map.Entry<InstanceType, Integer> entry =instanceRequestMap.getMaximumIterator().next();

		if(!entry.getKey().equals(defaultInstanceType)) {
			throw new InstanceException("Cannot allocate any other instances than the default.");
		}

		int noOfInstances = entry.getValue();
		for(int i=0; i<noOfInstances; i++) {
			Resource capability = Resource.newInstance(8192, 8);
			AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, null, null, null);
			amRmClient.addContainerRequest(request);
		}
	}

	@Override
	public void releaseAllocatedResource(JobID jobID, Configuration conf, AllocatedResource allocatedResource) throws InstanceException {

	}

	@Override
	public InstanceType getSuitableInstanceType(int minNumComputeUnits, int minNumCPUCores, int minMemorySize, int minDiskCapacity, int maxPricePerHour) {
		return null;
	}

	@Override
	public void reportHeartBeat(InstanceConnectionInfo instanceConnectionInfo, HardwareDescription hardwareDescription) {

	}

	@Override
	public InstanceType getInstanceTypeByName(String instanceTypeName) {
		return null;
	}

	@Override
	public InstanceType getDefaultInstanceType() {
		return null;
	}

	@Override
	public NetworkTopology getNetworkTopology(JobID jobID) {
		return null;
	}

	@Override
	public void setInstanceListener(InstanceListener instanceListener) {

	}

	@Override
	public Map<InstanceType, InstanceTypeDescription> getMapOfAvailableInstanceTypes() {
		return null;
	}

	@Override
	public AbstractInstance getInstanceByName(String name) {
		// TODO
		return null;
	}

	@Override
	public void cancelPendingRequests(JobID jobID) {

	}

	@Override
	public void shutdown() {
		try {
			amRmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Job Manager shutting down", null);
		} catch (Exception e) {
			LOG.error("Error shutting down AMRM Client", e);
		} finally {
			amRmClient.stop();
		}

		nmClient.stop();
	}

	@Override
	public int getNumberOfTaskTrackers() {
		return 0;
	}

	@Override
	public Map<InstanceConnectionInfo, ? extends AbstractInstance> getInstances() {
		return null;
	}
}
