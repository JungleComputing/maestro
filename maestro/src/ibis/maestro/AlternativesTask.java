package ibis.maestro;

/**
 * The interface of an alternatives task in the Maestro dataflow system.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public class AlternativesTask implements Task {
	private final Task alternatives[];

	AlternativesTask(Task... l) {
		alternatives = l;
	}

	/**
	 * Returns the name of these alternatives.
	 * 
	 * @return The name, composed of the names of the alternatives.
	 */
	@Override
	public String getName() {
		return "alternatives" + java.util.Arrays.deepToString(alternatives);
	}

	/**
	 * Returns true iff one of the alternatives is supported.
	 * 
	 * @return True iff one of the alternatives is supported.
	 */
	@Override
	public boolean isSupported() {
		// Supported if at least one alternative is supported.
		for (final Task t : alternatives) {
			if (t.isSupported()) {
				return true;
			}
		}
		return false; // None of the alternatives are supported.
	}
}
