package ibis.maestro;

/**
 * A simple synchronized up counter.
 * 
 * @author Kees van Reeuwijk.
 */
class Counter {
	private int value = 0;

	synchronized void add() {
		value++;
	}

	synchronized int get() {
		return value;
	}

	/**
	 * Returns a string representation of this counter. (Overrides method in
	 * superclass.)
	 * 
	 * @return The string representation.
	 */
	@Override
	public String toString() {
		return Integer.toString(get());
	}
}
