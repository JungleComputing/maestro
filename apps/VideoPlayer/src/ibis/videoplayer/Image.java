package ibis.videoplayer;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

abstract class Image implements Serializable {

    protected final int width;
    protected final int height;
    protected final int frameno;

    Image( int width, int height, int frameno )
    {
        this.width = width;
        this.height = height;
        this.frameno = frameno;
    }

    abstract Image scaleDown(int i);
    
    abstract void write( File f ) throws IOException;
}