/**
 * Builds a video fragment from the given range of frames.
 */
package ibis.videoplayer;

import ibis.maestro.Job;
import ibis.maestro.JobList;
import ibis.maestro.MapReduceHandler;
import ibis.maestro.MapReduceTask;

/**
 * @author Kees van Reeuwijk
 * 
 */
public final class BuildFragmentTask implements MapReduceTask {
    private static final long serialVersionUID = 6769001575637882594L;
    private Job fetchJob;
    int startFrame;
    int endFrame;
    RGB48Image frames[];

    BuildFragmentTask(Job fetchJob) {
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

    static Job createGetFrameJob(JobList jobs) {
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

    /* (non-Javadoc)
     * @see ibis.maestro.MapReduceTask#getResult()
     */
    public Object getResult() {
        int sz = 0;

        for (int i = 0; i < frames.length; i++) {
            RGB48Image frame = (RGB48Image) frames[i];
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

    /* (non-Javadoc)
     * @see ibis.maestro.MapReduceTask#map(java.lang.Object, ibis.maestro.MapReduceHandler)
     */
    public void map(Object input, MapReduceHandler handler) {
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

    /* (non-Javadoc)
     * @see ibis.maestro.MapReduceTask#reduce(java.lang.Object, java.lang.Object)
     */
    public void reduce(Object id, Object result) {
        // TODO Auto-generated method stub
        
    }

}
