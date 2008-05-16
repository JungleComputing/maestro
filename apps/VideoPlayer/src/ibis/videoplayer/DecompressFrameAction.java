package ibis.videoplayer;

import ibis.maestro.Node;

/**
 * An action to decompress a frame. We fake decompressing a video frame
 * by simply doubling the frame and repeating the content.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class DecompressFrameAction implements ibis.maestro.Job
{
    private static final long serialVersionUID = -3938044583266505212L;

    /** How many times should I repeat the fake decompression loop to approximate
     * the real decompression process.
     */
    private static final int REPEAT = 2;

    private void enlarge( short out[], short in[], int width, int height )
    {
        int outix = 0;

        for( int y=0; y<height; y++ ){
            int base = y*width;
            for( int ry = 0; ry<REPEAT; ry++ ){
                for( int x=0; x<width; x++ ){
                    int ix = base + x;
                    for( int rx = 0; rx<REPEAT; rx++ ){
                        out[outix++] = in[ix];
                    }
                }
            }
        }
    }

    /** Runs this job.
     * @return The decompressed frame.
     */
    @Override
    public Object run(Object obj, Node node )
    {
	Frame frame = (Frame) obj;
	short r[] = new short[frame.r.length*REPEAT*REPEAT];
	short g[] = new short[frame.g.length*REPEAT*REPEAT];
	short b[] = new short[frame.b.length*REPEAT*REPEAT];

        if( Settings.traceDecompressor ){
            System.out.println( "Decompressing frame " + frame.frameno );
        }
        enlarge( r, frame.r, frame.width, frame.height );
        enlarge( g, frame.g, frame.width, frame.height );
        enlarge( b, frame.b, frame.width, frame.height );
	return new Frame( frame.frameno, frame.width*REPEAT, frame.height*REPEAT, r, g, b );
    }
}
