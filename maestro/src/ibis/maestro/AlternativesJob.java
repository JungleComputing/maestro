package ibis.maestro;

/**
 * The interface of an alternatives job in the Maestro dataflow system.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public class AlternativesJob implements Job {
    final Job alternatives[];

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
