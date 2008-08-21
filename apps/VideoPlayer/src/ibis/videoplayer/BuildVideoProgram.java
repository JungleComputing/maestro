package ibis.videoplayer;

import ibis.maestro.JobCompletionListener;
import ibis.maestro.Job;
import ibis.maestro.JobList;
import ibis.maestro.Node;

/**
 * Small test program.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class BuildVideoProgram {
    private static final int OUTSTANDING_FRAGMENTS = 3*32;

    private static class Listener implements JobCompletionListener
    {
        int jobsCompleted = 0;
        private final int jobCount;
        private int runningJobs = 0;

        Listener( int jobCount )
        {
            this.jobCount = jobCount;
        }

        /** Handle the completion of job 'j': the result is 'result'.
         * @param id The job that was completed.
         * @param result The result of the job.
         */
        @Override
        public synchronized void jobCompleted( Node node, Object id, Object result ) {
            //System.out.println( "result is " + result );
            jobsCompleted++;
            runningJobs--;
            //System.out.println( "I now have " + jobsCompleted + "/" + jobCount + " jobs" );
            if( jobsCompleted>=jobCount ){
                System.out.println( "I got all job results back; stopping test program" );
                node.setStopped();
            }
            notifyAll();
        }

        synchronized void waitForRoom()
        {
            runningJobs++;
            while( runningJobs>OUTSTANDING_FRAGMENTS ){
                try {
                    wait();
                }
                catch (InterruptedException e) {
                    // Not interesting.
                }
            }
        }
    }

    @SuppressWarnings("synthetic-access")
    private void run( int frameCount, boolean goForMaestro ) throws Exception
    {
        JobList jobList = new JobList();
        // How many fragments will there be?
        int fragmentCount = (frameCount+Settings.FRAME_FRAGMENT_COUNT-1)/Settings.FRAME_FRAGMENT_COUNT;
        Listener listener = new Listener( fragmentCount );
	Job getFrameTask = BuildFragmentTask.createGetFrameJob( jobList );
	Job playTask = jobList.createJob( "videoplayer", new BuildFragmentTask( getFrameTask ) );

        Node node = new Node( jobList, goForMaestro );
        System.out.println( "Node created" );
        if( node.isMaestro() ) {
            System.out.println( "I am maestro; building a movie of " + frameCount + " frames" );
            for( int frame=0; frame<frameCount; frame += Settings.FRAME_FRAGMENT_COUNT ){
                final int endFrame = frame+Settings.FRAME_FRAGMENT_COUNT-1;
                FrameNumberRange range = new FrameNumberRange( frame, endFrame );
                listener.waitForRoom();
                playTask.submit( node, range, frame, listener );
            }
        }
        node.waitToTerminate();
    }

    /** The command-line interface of this program.
     * 
     * @param args The list of command-line parameters.
     */
    public static void main( String args[] )
    {
        boolean goForMaestro = true;
        int frameCount = 0;

        if( args.length == 0 ){
            System.err.println( "Missing parameter: I need a job count, or 'worker'" );
            System.exit( 1 );
        }
        String arg = args[0];
        if( arg.equalsIgnoreCase( "worker" ) ){
            goForMaestro = false;
        }
        else {
            frameCount = Integer.parseInt( arg );
        }
        System.out.println( "Running on platform " + Service.getPlatformVersion() + " args.length=" + args.length + " goForMaestro=" + goForMaestro + "; frameCount=" + frameCount );
        try {
            new BuildVideoProgram().run( frameCount, goForMaestro );
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }
}
