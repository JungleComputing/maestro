/**
 * 
 */
package ibis.videoplayer;

import java.io.Serializable;

class FrameNumberRange implements Serializable
{
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

    @Override
    public String toString()
    {
        return "[" + startFrameNumber + ".." + endFrameNumber + "]";
    }
}