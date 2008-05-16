package ibis.videoplayer;

import java.io.Serializable;

/**
 * A video frame.
 * 
 * @author Kees van Reeuwijk
 */
class Frame implements Serializable {
    private static final long serialVersionUID = 8797700803728846092L;
    final short r[];
    final short g[];
    final short b[];
    final int width;
    final int height;
    final int frameno;
    
    Frame( int frameno, int width, int height, short r[], short g[], short b[] ){
        this.width = width;
        this.height = height;
	this.r = r;
	this.g = g;
	this.b = b;
        this.frameno = frameno;
    }
    
    /**
     * Returns a string representation of this frame.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
	return "frame " + frameno + " " + width + "x" + height;
    }
}