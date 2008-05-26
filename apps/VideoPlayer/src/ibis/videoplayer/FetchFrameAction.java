package ibis.videoplayer;

import ibis.maestro.Context;
import ibis.maestro.Job;
import ibis.maestro.Node;

import java.io.File;
import java.io.IOException;

/**
 * A job to fetch a frame.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class FetchFrameAction implements Job
{
    private static final long serialVersionUID = -3938044583266505212L;

    /** Runs this action.
     * @return The frame we have fetched.
     */
    @Override
    public Object run( Object obj, Node node, Context context )
    {
	Integer frameno = (Integer) obj;
        File frameFile = new File( String.format( "frame-%04d.ppm" ) );
        Image frame;
        try {
            frame = UncompressedImage.load( frameFile, frameno );
        } catch (IOException e) {
            System.err.println( "Can not load frame '" + frameFile + "'" );
            e.printStackTrace();
            frame = null;
        }
        if( Settings.traceActions ) {
            System.out.println( "Fetched " + frame );
        }
	return frame;
    }
}
