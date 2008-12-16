package ibis.videoplayer;

import java.io.Serializable;

/**
 * A video fragment.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class VideoFragment implements Serializable {
    private static final long serialVersionUID = -791160275253169225L;
    final int startFrame;
    final int endFrame;
    final short data[];

    /**
     * Constructs a new video fragment.
     * 
     * @param startFrame
     *            The start frame of the fragment.
     * @param endFrame
     *            The end frame of the fragment.
     * @param data
     *            Pixel data
     */
    public VideoFragment(int startFrame, int endFrame, short data[]) {
        this.startFrame = startFrame;
        this.endFrame = endFrame;
        this.data = data;
    }

}
