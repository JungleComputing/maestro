package ibis.videoplayer;

import ibis.maestro.Job;
import ibis.maestro.Node;

/**
 * An action to color-correct a frame. We fake this by a video frame
 * by simply doubling the frame and repeating the content.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class ColourCorrectJob implements Job
{
    private static final long serialVersionUID = -3938044583266505212L;

    /** Runs this job.
     * @return The decompressed frame.
     */
    @Override
    public Object run(Object obj, Node node )
    {
	RGB48Image frame = (RGB48Image) obj;
        return frame.colourCorrect(
                0.0, 0.0, 1.0,
                0.0, 1.0, 0.0,
                1.0, 0.0, 0.0
        );

    }


    /**
     * @param context The program context.
     * @return True, because this job can run anywhere.
     */
    @Override
    public boolean isSupported()
    {
	return true;
    }
}