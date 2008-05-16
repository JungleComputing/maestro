/**
 * Builds a video fragment from the given range of frames.
 */
package ibis.videoplayer;

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
    public Object run( Object obj, Node node )
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
        int szr = 0;
        int szg = 0;
        int szb = 0;
        for( int i=0; i<res.length; i++ ){
            Frame frame = (Frame) res[i];
            if( frame != null ){
                szr += frame.r.length;
                szg += frame.g.length;
                szb += frame.b.length;
            }
        }
        short r[] = new short[szr];
        short g[] = new short[szg];
        short b[] = new short[szb];
        int ixr = 0;
        int ixg = 0;
        int ixb = 0;
        for( int i=0; i<res.length; i++ ){
            Frame frame = (Frame) res[i];
            if( frame != null ){
                System.arraycopy( frame.r, 0, r, ixr, frame.r.length );
                ixr += frame.r.length;
                System.arraycopy( frame.g, 0, g, ixg, frame.g.length );
                ixg += frame.g.length;
                System.arraycopy( frame.b, 0, b, ixb, frame.b.length );
                ixb += frame.b.length;
            }
        }
        VideoFragment value = new VideoFragment( startFrame, endFrame, r, g, b );
        if( Settings.traceFragmentBuilder ){
            System.out.println( "Sending fragment [" + startFrame + "..." + endFrame + "]" );
        }
        return value;
    }

    static Task createGetFrameTask( Node node )
    {
        return node.createTask(
                "getFrame",
                new FetchFrameAction(),
                new DecompressFrameAction(),
                new ColorCorrectAction(),
                new ScaleFrameAction()
        );
    }

}
