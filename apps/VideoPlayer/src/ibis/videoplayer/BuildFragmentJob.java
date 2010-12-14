/**
 * Builds a video fragment from the given range of frames.
 */
package ibis.videoplayer;

import ibis.maestro.Job;
import ibis.maestro.ParallelJob;
import ibis.maestro.ParallelJobContext;
import ibis.maestro.ParallelJobHandler;
import ibis.maestro.ParallelJobInstance;
import ibis.maestro.SeriesJob;

import java.io.Serializable;

/**
 * @author Kees van Reeuwijk
 * 
 */
final class BuildFragmentJob implements ParallelJob {
    private static final long serialVersionUID = 6769001575637882594L;
    private final Job fetchJob;

    BuildFragmentJob(Job fetchJob) {
        this.fetchJob = fetchJob;
    }

    static SeriesJob createGetFrameJob() {
        return new SeriesJob(new FetchFrameJob(), new DecompressFrameJob(),
                new ColourCorrectJob(), new ScaleFrameJob(2));
    }

    /**
     * @return True, because this job can run anywhere.
     */
    @Override
    public boolean isSupported() {
        return true;
    }

    static class BuildFragmentInstance extends ParallelJobInstance {
        RGB48Image frames[];
        int startFrame;
        int endFrame;
        private final Job fetchJob;

        public BuildFragmentInstance(ParallelJobContext context, Job fetchJob) {
            super(context);
            this.fetchJob = fetchJob;
        }

        /**
         * Returns the result of the reduction.
         * 
         * @return The result.
         */
        @Override
        public Serializable getResult() {
            int sz = 0;

            for (int i = 0; i < frames.length; i++) {
                final RGB48Image frame = frames[i];
                if (frame != null) {
                    sz += frame.data.length;
                }
            }
            final short data[] = new short[sz];
            int ix = 0;
            for (int i = 0; i < frames.length; i++) {
                final RGB48Image frame = frames[i];
                if (frame != null) {
                    System.arraycopy(frame.data, 0, data, ix, frame.data.length);
                    ix += frame.data.length;
                }
            }
            final VideoFragment value = new VideoFragment(startFrame, endFrame,
                    data);
            if (Settings.traceFragmentBuilder) {
                System.out.println("Sending fragment [" + startFrame + "..."
                        + endFrame + "]");
            }
            return value;
        }

        /**
         * Handle results as they arrive.
         * 
         * @param id
         *            The id of the result.
         * @param result
         *            The result.
         */
        @Override
        public void merge(Serializable id, Serializable result) {
            final int ix = (Integer) id;
            frames[ix] = (RGB48Image) result;
        }

        @Override
        public boolean resultIsReady() {
            for (final RGB48Image i : frames) {
                if (i == null) {
                    // Frame is not filled in yet, so no.
                    return false;
                }
            }
            // All frames are filled in, we're ready.
            return true;
        }

        /**
         * @param input
         *            The input for the computation.
         * @param handler
         *            The map/reduce handler assigned to this computation.
         */
        @Override
        public void split(Serializable input, ParallelJobHandler handler) {
            final FrameNumberRange range = (FrameNumberRange) input;
            if (Settings.traceFragmentBuilder) {
                System.out.println("Collecting frames for fragment " + range);
            }
            startFrame = range.startFrameNumber;
            endFrame = range.endFrameNumber;
            frames = new RGB48Image[1 + endFrame - startFrame];
            for (int frame = startFrame; frame <= endFrame; frame++) {
                final Integer frameno = new Integer(frame);
                handler.submit(frameno, this, frameno, fetchJob);
            }
        }

    }

    @Override
    public ParallelJobInstance createInstance(ParallelJobContext context) {
        return new BuildFragmentInstance(context, fetchJob);
    }
}
