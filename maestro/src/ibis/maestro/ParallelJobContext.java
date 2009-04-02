package ibis.maestro;

/**
 * A class holding some administration stuff for parallel job instances.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public class ParallelJobContext {
    final RunJobMessage message;
    final double runMoment;

    ParallelJobContext(RunJobMessage message, double runMoment) {
        this.message = message;
        this.runMoment = runMoment;
    }

    long [] getPrefix()
    {
    	return message.jobInstance.jobInstance.ids;
    }
}
