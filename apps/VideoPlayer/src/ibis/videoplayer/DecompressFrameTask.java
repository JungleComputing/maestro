package ibis.videoplayer;

import ibis.maestro.AtomicTask;
import ibis.maestro.Node;

/**
 * A task to decompress a frame. We fake decompressing a video frame
 * by simply doubling the frame and repeating the content.
 * FIXME: implement this properly.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class DecompressFrameTask implements AtomicTask
{
    private static final long serialVersionUID = -3938044583266505212L;

    /** How many times should I repeat the fake decompression loop to approximate
     * the real decompression process.
     */
    private static final int REPEAT = 2;

	/**
	 * Returns the name of this task.
	 * @return The name.
	 */
	@Override
	public String getName()
	{
	    return "Decompress frame";
	}

    /** Runs this job.
     * @return The decompressed frame.
     */
    @Override
    public Object run( Object obj, Node node )
    {
	RGB48Image frame = (RGB48Image) obj;
        short in[] = frame.data;
	short data[] = new short[in.length*REPEAT*REPEAT];

        if( Settings.traceDecompressor ){
            System.out.println( "Decompressing frame " + frame.frameno );
        }
        int outix = 0;

        // FIXME: do something sane here.
        for( int y=0; y<frame.height; y++ ){
            int base = y*frame.width;

            for( int ry = 0; ry<REPEAT; ry++ ){
                for( int x=0; x<frame.width; x++ ){
                    int ix = base + x;
                    for( int rx = 0; rx<REPEAT; rx++ ){
                        data[outix++] = in[ix];
                    }
                }
            }
        }
	return new RGB48Image( frame.frameno, frame.width*REPEAT, frame.height*REPEAT, data );
    }

    /**
     * @return True, because this job can run anywhere.
     */
    @Override
    public boolean isSupported()
    {
	return true;
    }
}
