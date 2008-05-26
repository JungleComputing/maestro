package ibis.videoplayer;

import ibis.maestro.Context;
import ibis.maestro.Job;
import ibis.maestro.Node;

/**
 * A job to fetch and scale a frame.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class ScaleFrameAction implements Job
{
    private static final long serialVersionUID = -3938044583266505212L;

    /**
     * Scales the given input, an image, and returns a scaled version of the image.
     * @param obj The input of this function.
     * @param node Some info of the node we're running this on.
     * @return The scaled image.
     */
    @Override
    public Object run( Object obj, Node node, Context context )
    {
	Image frame = (Image) obj;
        return frame.scaleDown( 2 );
    }
}
