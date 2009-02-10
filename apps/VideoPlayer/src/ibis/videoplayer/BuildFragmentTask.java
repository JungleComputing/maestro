/**
 * Builds a video fragment from the given range of frames.
 */
package ibis.videoplayer;

import ibis.maestro.JobList;
import ibis.maestro.JobSequence;
import ibis.maestro.ParallelJob;
import ibis.maestro.ParallelJobHandler;

/**
 * @author Kees van Reeuwijk
 * 
 */
public final class BuildFragmentTask implements ParallelJob {
    private static final long serialVersionUID = 6769001575637882594L;
    private JobSequence fetchJob;
    int startFrame;
    int endFrame;
    RGB48Image frames[];

    BuildFragmentTask(JobSequence fetchJob) {
        this.fetchJob = fetchJob;
    }

    /**
     * Returns the name of this task.
     * 
     * @return The name.
     */
    @Override
    public String getName() {
        return "Build fragment";
    }

    static JobSequence createGetFrameJob(JobList jobs) {
        return jobs.createJob("getFrame", new FetchFrameTask(),
                new DecompressFrameTask(), new ColourCorrectTask(),
                new ScaleFrameTask(2));
    }

    /**
     * @return True, because this job can run anywhere.
     */
    @Override
    public boolean isSupported() {
        return true;
    }

    /**
     * Returns the result of the reduction.
     * @return The result.
     */
    public Object getResult() {
        int sz = 0;

        for (int i = 0; i < frames.length; i++) {
            RGB48Image frame = frames[i];
            if (frame != null) {
                sz += frame.data.length;
            }
        }
        short data[] = new short[sz];
        int ix = 0;
        for (int i = 0; i < frames.length; i++) {
            RGB48Image frame = frames[i];
            if (frame != null) {
                System.arraycopy(frame.data, 0, data, ix, frame.data.length);
                ix += frame.data.length;
            }
        }
        VideoFragment value = new VideoFragment(startFrame, endFrame, data);
        if (Settings.traceFragmentBuilder) {
            System.out.println("Sending fragment [" + startFrame + "..."
                    + endFrame + "]");
        }
        return value;
    }

    /**
     * @param input The input for the computation.
     * @param handler The map/reduce handler assigned to this computation.
     */
    public void map(Object input, ParallelJobHandler handler) {
        FrameNumberRange range = (FrameNumberRange) input;
        if (Settings.traceFragmentBuilder) {
            System.out.println("Collecting frames for fragment " + range);
        }
        startFrame = range.startFrameNumber;
        endFrame = range.endFrameNumber;
        frames = new RGB48Image[1+endFrame-startFrame];
        for (int frame = startFrame; frame <= endFrame; frame++) {
            Integer frameno = new Integer(frame);
            handler.submit(frameno, frameno, true, fetchJob);
        }
    }

    /**
     * Handle results as they arrive.
     * @param id The id of the result.
     * @param result The result.
     */
    public void reduce(Object id, Object result) {
        int ix = (Integer) id;
        frames[ix] = (RGB48Image) result;
    }
}
