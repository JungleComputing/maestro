package ibis.videoplayer;

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

    @Override
    public Object run( Object obj, Node node )
    {
	Image frame = (RGB48Image) obj;
        return frame.scale();
    }
}
