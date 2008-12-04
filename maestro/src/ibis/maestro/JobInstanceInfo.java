// File: $Id: $

package ibis.maestro;

class JobInstanceInfo {
    final JobInstanceIdentifier identifier;
    final Job job;
    final JobCompletionListener listener;
    final long startTime = System.nanoTime();

    /**
     * Constructs an information class for the given job identifier.
     * 
     * @param identifier
     *            The job identifier.
     * @param job
     *            The job this belongs to.
     * @param listener
     *            The completion listener associated with the job.
     */
    JobInstanceInfo(final JobInstanceIdentifier identifier, Job job,
	    final JobCompletionListener listener) {
	this.identifier = identifier;
	this.job = job;
	this.listener = listener;
    }
}