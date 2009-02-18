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
                runningJobs.remove(i);
                return job;
            }
        }
        return null;
    }

    /** Returns the earliest late job. */
    synchronized JobInstance getLateJob() {
        JobInstanceInfo earliest = null;
        for (int i = 0; i < runningJobs.size(); i++) {
            final JobInstanceInfo job = runningJobs.get(i);

            if (earliest == null || earliest.startTime > job.startTime) {
                earliest = job;
            }
        }
        final double now = Utils.getPreciseTime();
        final double lateDeadline = now - Settings.LATE_JOB_DURATION;
        if (earliest == null || earliest.startTime >= lateDeadline) {
            // There are no running jobs, or even the earliest is recent.
            return null;
        }
        earliest.startTime = now;
        return earliest.jobInstance;
    }

    synchronized void clear() {
        runningJobs.clear();
    }
}
