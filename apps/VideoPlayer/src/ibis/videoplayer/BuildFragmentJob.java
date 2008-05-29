/**
 * Builds a video fragment from the given range of frames.
 */
package ibis.videoplayer;

import ibis.maestro.Context;
import ibis.maestro.Job;
import ibis.maestro.Node;
import ibis.maestro.Task;
import ibis.maestro.TaskWaiter;

/**
 * @author Kees van Reeuwijk
 *
 */
public final class BuildFragmentJob implements Job
{
    /** */
    private static final long serialVersionUID = 6769001575637882594L;
    private Task fetchTask;

    BuildFragmentJob( Task fetchTask )
    {
        this.fetchTask = fetchTask;
    }

    /**
     * Runs this fragment building job.
     * @param obj The input to this task.
     * @param node The node this job is running on.
     */
    @Override
    public Object run( Object obj, Node node, Context context )
    {
        TaskWaiter waiter = new TaskWaiter();

        FrameNumberRange range = (FrameNumberRange) obj;
        int startFrame = range.startFrameNumber;
        int endFrame = range.endFrameNumber;

        if( Settings.traceFragmentBuilder ){
            System.out.println( "Collecting frames for fragment " + range  );
        }
        for( int frame=startFrame; frame<=endFrame; frame++ ) {
            Integer frameno = new Integer( frame );
            waiter.submit( fetchTask, frameno );
        }
        Object res[] = waiter.sync( node );
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

    static Task createGetFrameTask( Node node )
    {
        return node.createTask(
                "getFrame",
                new FetchFrameJob(),
                new DecompressFrameJob(),
                new ColourCorrectJob(),
                new ScaleFrameJob()
        );
    }

}
