package ibis.videoplayer;

import java.io.Serializable;

import ibis.maestro.Context;
import ibis.maestro.Node;

/**
 * An action to run a PovRay scene description through povray, and load the
 * resulting image.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class RenderFrameJob implements ibis.maestro.Job
{
    private static final long serialVersionUID = -3938044583266505212L;

    private final String iniFile;
    
    /**
     * @param iniFile
     */
    RenderFrameJob( String iniFile )
    {
	this.iniFile = iniFile;
    }

    class RenderInfo implements Serializable
    {
	final int width;
	final int height;
	final int frame;
	final String scene;

	/**
	 * @param width
	 * @param height
	 * @param frame
	 * @param scene
	 */
	RenderInfo(int width, int height, int frame, String scene) {
	    this.width = width;
	    this.height = height;
	    this.frame = frame;
	    this.scene = scene;
	}
    }
    
    private static void writeFile( String filename, String content )
    {

    }

    /** Runs this job.
     * @return The decompressed frame.
     */
    @Override
    public Object run( Object obj, Node node, Context context )
    {
	
	RenderInfo info = (RenderInfo) obj;
	
	writeFile( String.format( "frame-%06d.pov", info.frame ), info.scene );
	writeFile( String.format( "frame-%06d.ini", info.frame ), iniFile );
	String command[] = {
		"povray",
		"+FP",
		
	};
	return null;
    }
}
