package ibis.videoplayer;

import ibis.maestro.AtomicJob;

import java.io.File;
import java.io.IOException;

/**
 * A task to fetch a frame.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public class FetchFrameTask implements AtomicJob {
    private static final long serialVersionUID = -3938044583266505212L;

    /**
     * Runs this task.
     * 
     * @return The frame we have fetched.
     */
    @Override
    public Object run(Object obj) {
        Integer frameno = (Integer) obj;
        File frameFile = new File(String.format("frame-%04d.ppm",frameno));
        Image frame;
        try {
            frame = Image.load(frameFile, frameno);
        } catch (IOException e) {
            System.err.println("Can not load frame '" + frameFile + "'");
            e.printStackTrace();
            frame = null;
        }
        if (Settings.traceJobs) {
            System.out.println("Fetched " + frame);
        }
        return frame;
    }

    /**
     * @return True, because this task can run anywhere.
     */
    @Override
    public boolean isSupported() {
        return true;
    }
}
