package ibis.maestro;

import ibis.steel.Estimate;

import java.util.Arrays;

/**
 * Local information about a node.
 * 
 * @author Kees van Reeuwijk.
 */
class LocalNodeInfoList {
	final boolean suspect;

	private final LocalNodeInfo infoPerType[];

	LocalNodeInfoList(final boolean suspect, final LocalNodeInfo[] infoPerType) {
		this.suspect = suspect;
		this.infoPerType = infoPerType;
	}

	/**
	 * Given a job type, returns local performance info for that job type.
	 * 
	 * @param type
	 *            The type of job we want the info for.
	 * @return The local performance info.
	 */
	LocalNodeInfo getLocalNodeInfo(final JobType type) {
		return infoPerType[type.index];
	}

	Estimate getTransmissionTime(final int ix) {
		return infoPerType[ix].transmissionTime;
	}

	/**
	 * Given a job type, returns a reasonable deadline for execution on the
	 * local node.
	 * 
	 * @param type
	 *            The type of job we want a deadline for.
	 * @return The deadline of the job in seconds.
	 */
	double getDeadline(final JobType type) {
		return infoPerType[type.index].predictedDuration
				.getHighEstimate();
	}

	@Override
	public String toString() {
		return Arrays.deepToString(infoPerType);
	}
}
