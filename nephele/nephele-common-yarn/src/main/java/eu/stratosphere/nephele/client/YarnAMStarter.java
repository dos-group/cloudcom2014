package eu.stratosphere.nephele.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Starts Nephele's job manager as YARN's application master.
 */
public class YarnAMStarter {

	private static final Log LOG = LogFactory.getLog(YarnAMStarter.class);

	public static void main(String[] args) throws IOException, YarnException {
		if (args.length != 1) {
			printUsage();
			System.exit(1);
			return;
		}

		YarnConfiguration conf = new YarnConfiguration();
		conf.set(YarnConfiguration.RM_ADDRESS, args[0]);

		YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		yarnClient.start();

		YarnClientApplication app = yarnClient.createApplication();

		// set the application name
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		ApplicationId appId = appContext.getApplicationId();

		appContext.setKeepContainersAcrossApplicationAttempts(false);
		appContext.setApplicationName("Nephele Streaming");


		// Set up the container launch context for the application master
		List<String> commands = new ArrayList<String>();
		commands.add("/home/bjoern/stratosphere/bin/nephele-jobmanager.sh start yarn");

		HashMap<String, String> env = new HashMap<String, String>();
		env.put("NEPHELE_YARN_AM_RM_HOSTNAME", "wally091");

		ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
						new HashMap<String, LocalResource>(), env, commands, null, null, null);
		appContext.setAMContainerSpec(amContainer);

		// Set up resource type requirements
		// For now, both memory and vcores are supported, so we set memory and
		// vcores requirements
		int amMemoryMegabytes = 2000;
		int amVcores = 1;
		appContext.setResource(Resource.newInstance(amMemoryMegabytes, amVcores));

		// Submit the application to the applications manager
		// SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
		// Ignore the response as either a valid response object is returned on success
		// or an exception thrown to denote some form of a failure
		LOG.info("Submitting application to RM");
		yarnClient.submitApplication(appContext);

		monitorApplication(yarnClient, appId);
	}


	private static void monitorApplication(YarnClient yarnClient, ApplicationId appId) throws YarnException, IOException {

		boolean continueMonitoring = true;
		while (continueMonitoring) {
			// Check app status every 1 second.
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				LOG.debug("Thread sleep in monitoring loop interrupted");
			}

			// Get application report for the appId we are interested in
			ApplicationReport report = yarnClient.getApplicationReport(appId);

			LOG.info("Got application report from RM for"
							+ ", appId=" + appId.getId()
							+ ", appDiagnostics=" + report.getDiagnostics()
							+ ", appMasterHost=" + report.getHost()
							+ ", appMasterRpcPort=" + report.getRpcPort()
							+ ", appQueue=" + report.getQueue()
							+ ", appStartTime=" + report.getStartTime()
							+ ", yarnAppState=" + report.getYarnApplicationState().toString()
							+ ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
							+ ", appTrackingUrl=" + report.getTrackingUrl()
							+ ", appUser=" + report.getUser());

			YarnApplicationState state = report.getYarnApplicationState();
			FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();

			switch(state) {
				case FINISHED:

					if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
						LOG.info("Application has completed successfully. Breaking monitoring loop");
					} else {
						LOG.info("Application did finished unsuccessfully."
										+ " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
										+ ". Breaking monitoring loop");
					}

					continueMonitoring = false;
					break;
				case KILLED:
				case FAILED:
					LOG.info("Application did not finish."
									+ " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
									+ ". Breaking monitoring loop");
					continueMonitoring = false;
					break;
			}
		}
	}

	private static void printUsage() {
		System.err.println("Parameters: <yarn-resourcemanager-host>:<port>");
	}

}
