package ibis.videoplayer;

import ibis.maestro.AtomicJob;

import java.io.Serializable;

/**
 * An action to color-correct a frame. We fake this by a video frame by simply
 * doubling the frame and repeating the content.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public class ColourCorrectJob implements AtomicJob {
    private static final long serialVersionUID = -3938044583266505212L;

    /**
     * Runs this job.
     * 
     * @return The decompressed frame.
     */
    @Override
    public Serializable run(Object obj) {
        final RGB48Image frame = (RGB48Image) obj;
        return frame.colourCorrect(0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 0.0);

    }

    /**
     * @return True, because this job can run anywhere.
     */
    @Override
    public boolean isSupported() {
        return true;
    }
}
