package ibis.videoplayer;

import ibis.maestro.AtomicTask;
import ibis.maestro.Node;

/**
 * An action to color-correct a frame. We fake this by a video frame
 * by simply doubling the frame and repeating the content.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class ColourCorrectTask implements AtomicTask
{
    private static final long serialVersionUID = -3938044583266505212L;

    /**
     * Returns the name of this task.
     * @return The name.
     */
    @Override
    public String getName()
    {
	return "Colour-correct frame";
    }

    /** Runs this task.
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
     * @return True, because this task can run anywhere.
     */
    @Override
    public boolean isSupported()
    {
	return true;
    }
}
