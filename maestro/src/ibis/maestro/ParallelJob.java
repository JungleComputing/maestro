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
     * @param context The context of this job instance.
     * @return The newly created job instance.
     */
    public ParallelJobInstance createInstance(ParallelJobContext context);
}
