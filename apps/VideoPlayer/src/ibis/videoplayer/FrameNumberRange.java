/**
 * 
 */
package ibis.videoplayer;

import java.io.Serializable;

class FrameNumberRange implements Serializable
{
    private static final long serialVersionUID = -7295339911112467893L;
    final int startFrameNumber;
    final int endFrameNumber;

    /**
     * @param startFrameNumber
     * @param endFrameNumber
     */
    public FrameNumberRange(int startFrameNumber, int endFrameNumber) {
        this.startFrameNumber = startFrameNumber;
        this.endFrameNumber = endFrameNumber;
    }

    /** Returns a string
     * representation of this frame number range.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "[" + startFrameNumber + ".." + endFrameNumber + "]";
    }
}