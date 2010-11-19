/**
 * 
 */
package ibis.maestro;

import ibis.steel.Estimate;

class LocalNodeInfo {
	final int currentJobs;
	final Estimate transmissionTime;
	final Estimate predictedDuration;

	/**
	 * @param currentJobs
	 * @param transmissionTime
	 * @param predictedDuration
	 */
	LocalNodeInfo(final int currentJobs, final Estimate transmissionTime,
			final Estimate predictedDuration) {
		this.currentJobs = currentJobs;
		this.transmissionTime = transmissionTime;
		this.predictedDuration = predictedDuration;
	}

	@Override
	public String toString() {
		return "(currentJobs=" + currentJobs + ",transmissionTime="
				+ Utils.formatSeconds(transmissionTime) + ",predictedDuration="
				+ Utils.formatSeconds(predictedDuration) + ")";

	}
}
