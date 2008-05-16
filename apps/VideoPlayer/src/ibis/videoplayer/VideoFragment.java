package ibis.videoplayer;

import java.io.Serializable;

/** A video fragment.
 * 
 * @author Kees van Reeuwijk
 *
 */
class VideoFragment implements Serializable
{
    private static final long serialVersionUID = -791160275253169225L;
    final int startFrame;
    final int endFrame;
    final short r[];
    final short g[];
    final short b[];

    /**
     * Constructs a new video fragment.
     * @param startFrame The start frame of the fragment.
     * @param endFrame The end frame of the fragment.
     * @param r Red components
     * @param g Green components
     * @param b Blue components
     */
    public VideoFragment( int startFrame, int endFrame, short r[], short g[], short b[] )
    {
        this.startFrame = startFrame;
        this.endFrame = endFrame;
        this.r = r;
        this.g = g;
        this.b = b;
    }

}
