package ibis.maestro;

/**
 * The interface of a parallel job in the Maestro workflow system.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public interface ParallelJob extends Job {
    /**
     * Given some administration stuff, create a new instance of a parallel job.
     * @param message The job message belonging to this instance.
     * @param runMoment The moment the job was run.
     * @return The newly created job instance.
     */
    public ParallelJobInstance createInstance(RunJobMessage message,
            double runMoment);
}
