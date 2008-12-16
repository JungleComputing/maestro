
package ibis.videoplayer;

/**
 * Settings for the video player.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class Settings {
	/** The number of frames in a fragment. */
	public static final int FRAME_FRAGMENT_COUNT = 10;

	// ----------------------

	static final boolean traceFetcher = false;
	static final boolean traceFragmentBuilder = false;
	static final boolean traceScaler = false;
	static final boolean traceDecompressor = false;

	static boolean traceJobs = false;

	static final int FRAME_HEIGHT = 2160;
	static final int FRAME_WIDTH = 3480;
	static final String RANK = "PRUN_CPU_RANK";
}
