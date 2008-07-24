/**
 * Builds a video fragment from the given range of frames.
 */
package ibis.videoplayer;

import ibis.maestro.AtomicTask;
import ibis.maestro.Job;
import ibis.maestro.JobList;
import ibis.maestro.JobWaiter;
import ibis.maestro.Node;

/**
 * @author Kees van Reeuwijk
 *
 */
public final class BuildFragmentTask implements AtomicTask
{
    private static final long serialVersionUID = 6769001575637882594L;
    private Job fetchJob;

    BuildFragmentTask( Job fetchJob )
    {
        this.fetchJob = fetchJob;
    }

    /**
     * Runs this fragment building job.
     * @param obj The input to this task.
     * @param node The node this job is running on.
     */
    @Override
    public Object run( Object obj, Node node )
    {
        JobWaiter waiter = new JobWaiter();

        FrameNumberRange range = (FrameNumberRange) obj;
        int startFrame = range.startFrameNumber;
        int endFrame = range.endFrameNumber;

        if( Settings.traceFragmentBuilder ){
            System.out.println( "Collecting frames for fragment " + range  );
        }
        for( int frame=startFrame; frame<=endFrame; frame++ ) {
            Integer frameno = new Integer( frame );
            waiter.submit( node, fetchJob, frameno );
        }
        // FIXME: run another tread during the sync.
        Object res[] = waiter.sync();
        if( Settings.traceFragmentBuilder ){
            System.out.println( "Building fragment [" + startFrame + "..." + endFrame + "]" );
        }
        int sz = 0;
        for( int i=0; i<res.length; i++ ){
            RGB48Image frame = (RGB48Image) res[i];
            if( frame != null ){
                sz += frame.data.length;
            }
        }
        short data[] = new short[sz];
        int ix = 0;
        for( int i=0; i<res.length; i++ ){
            RGB48Image frame = (RGB48Image) res[i];
            if( frame != null ){
                System.arraycopy( frame.data, 0, data, ix, frame.data.length );
                ix += frame.data.length;
            }
        }
        VideoFragment value = new VideoFragment( startFrame, endFrame, data );
        if( Settings.traceFragmentBuilder ){
            System.out.println( "Sending fragment [" + startFrame + "..." + endFrame + "]" );
        }
        return value;
    }

    static Job createGetFrameJob( JobList jobs )
    {
        return jobs.createJob(
                "getFrame",
                new FetchFrameTask(),
                new DecompressFrameTask(),
                new ColourCorrectTask(),
                new ScaleFrameTask( 2 )
        );
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
