
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

    static boolean traceActions = false;

    static final int FRAME_HEIGHT = 400;
    static final int FRAME_WIDTH = 750;
}
