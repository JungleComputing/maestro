/**
 * 
 */
package ibis.maestro;

import ibis.steel.Estimator;

class LocalNodeInfo {
	final int currentJobs;
	final Estimator transmissionTime;
	final Estimator predictedDuration;

	/**
	 * @param currentJobs
	 * @param transmissionTime2
	 * @param predictedDuration
	 */
	LocalNodeInfo(final int currentJobs, final Estimator transmissionTime2,
			final Estimator predictedDuration) {
		this.currentJobs = currentJobs;
		this.transmissionTime = transmissionTime2;
		this.predictedDuration = predictedDuration;
	}

	@Override
	public String toString() {
		return "(currentJobs=" + currentJobs + ",transmissionTime="
				+ Utils.formatSeconds(transmissionTime) + ",predictedDuration="
				+ Utils.formatSeconds(predictedDuration) + ")";

	}
}
