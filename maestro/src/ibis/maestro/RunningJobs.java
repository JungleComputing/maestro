package ibis.maestro;

import java.util.ArrayList;

/**
 * The list of running jobs.
 * 
 * @author Kees van Reeuwijk.
 */
class RunningJobs {
	private final ArrayList<JobInstanceInfo> runningJobs = new ArrayList<JobInstanceInfo>();

	synchronized void add(JobInstanceInfo info) {
		runningJobs.add(info);
	}

	synchronized JobInstanceInfo remove(JobInstanceIdentifier id) {
		for (int i = 0; i < runningJobs.size(); i++) {
			final JobInstanceInfo job = runningJobs.get(i);
			if (job.identifier.equals(id)) {
				final long jobInterval = System.nanoTime() - job.startTime;
				job.job.registerJobTime(jobInterval);
				runningJobs.remove(i);
				return job;
			}
		}
		return null;
	}

	/** Returns the earliest late job. */
	synchronized TaskInstance getLateJob() {
		JobInstanceInfo earliest = null;
		for (int i = 0; i < runningJobs.size(); i++) {
			final JobInstanceInfo job = runningJobs.get(i);

			if( earliest == null || earliest.startTime>job.startTime ){
				earliest = job;
			}
		}
		final long now = System.nanoTime();
		final long lateDeadline = now-Settings.LATE_JOB_DURATION;
		if( earliest == null || earliest.startTime>=lateDeadline ){
			// There are no running jobs, or even the earliest is recent.
			return null;
		}
		earliest.startTime = now;
		return earliest.taskInstance;
	}
}
