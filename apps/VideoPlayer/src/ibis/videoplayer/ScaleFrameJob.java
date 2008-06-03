package ibis.videoplayer;

import ibis.maestro.Job;
import ibis.maestro.Node;

/**
 * A job to fetch and scale a frame.
 * 
 * @author Kees van Reeuwijk
 *
 */
class ScaleFrameJob implements Job
{
    private static final long serialVersionUID = -3938044583266505212L;
    private final int factor;
    
    ScaleFrameJob( int factor )
    {
        this.factor = factor;
    }

    /**
     * Scales the given input, an image, and returns a scaled version of the image.
     * @param obj The input of this function.
     * @param node Some info of the node we're running this on.
     * @return The scaled image.
     */
    @Override
    public Object run( Object obj, Node node )
    {
	Image frame = (Image) obj;
        System.out.println( "Scaling " + frame );
        return frame.scaleDown( factor );
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