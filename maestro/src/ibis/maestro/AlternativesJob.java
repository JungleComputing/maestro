package ibis.maestro;

/**
 * The interface of an alternatives job in the Maestro dataflow system.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public class AlternativesJob implements Job {
    final Job alternatives[];

    /**
     * Creates a job that selects one of the given list of jobs to
     * execute. Presumably all alternatives implement the same functionality,
     * but the performance of each of them may be different on different
     * platforms.
     * 
     * @param l The list of alternative implementations.
     */
    public AlternativesJob(Job... l) {
        alternatives = l;
    }

    /**
     * Returns true iff one of the alternatives is supported.
     * 
     * @return True iff one of the alternatives is supported.
     */
    @Override
    public boolean isSupported() {
        // Supported if at least one alternative is supported.
        for (final Job t : alternatives) {
            if (t.isSupported()) {
                return true;
            }
        }
        return false; // None of the alternatives are supported.
    }
}
