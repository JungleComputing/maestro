package ibis.maestro;

/**
 * A single worker thread of a Maestro worker.
 * 
 * @author Kees van Reeuwijk.
 */
final class WorkThread extends Thread {
	private final Node node;

	/**
	 * Given a work source, constructs a new WorkThread.
	 * 
	 * @param source
	 *            The work source.
	 * @param node
	 *            The local node.
	 */
	WorkThread(Node node) {
		super("Work thread");
		setDaemon(true);
		this.node = node;
	}

	/**
	 * Run this thread: keep getting and executing tasks until a null task is
	 * returned.
	 */
	@Override
	public void run() {
		node.runWorkThread();
		if (Settings.traceNodeProgress) {
			Globals.log.reportProgress("Work thread " + this + " ended");
		}

	}
}
