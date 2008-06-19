package ibis.videoplayer;

import ibis.maestro.Job;
import ibis.maestro.JobList;
import ibis.maestro.JobWaiter;
import ibis.maestro.Node;
import ibis.maestro.Task;

import java.io.File;
import java.io.IOException;

/**
 * Compare pairs of images in different directories for changes.
 * 
 * @author Kees van Reeuwijk
 *
 */
class FindNovaProgram
{

    /**
     * @param file The file or directory to submit.
     * @param waiter The waiter thread that will wait for the return of the results.
     * @param searchTask The job to submit the files to.
     */
    private void submitAll( Node node, File file, JobWaiter waiter, Job searchTask )
    {
        if( file.isDirectory() ) {
            File files[] = file.listFiles();
            for( File f: files ) {
                submitAll( node, f, waiter, searchTask );
            }
        }
        else {
            try {
                UncompressedImage img = UncompressedImage.load( file, 0 );
                CompareImageTask.ImageMatches im = new CompareImageTask.ImageMatches( img, file );
                waiter.submit( node, searchTask, im );
            }
            catch( IOException e ) {
                System.err.println( "Cannot load image '" + file + "': " + e.getLocalizedMessage() );
            }
        }
    }

    private void run( File subjectDirectory, String databaseList[] ) throws Exception
    {
        JobList jobList = new JobList();
        JobWaiter waiter = new JobWaiter();

        Task tasks[] = new Task[databaseList.length];
        int ix = 0;
        for( String db: databaseList ) {
            File dbf = new File( db );
            tasks[ix++] = new CompareImageTask( dbf );
        }
        Job searchTask =  jobList.createJob( "databaseSearch", tasks );
        Node node = new Node( jobList, subjectDirectory != null );
        System.out.println( "Node created" );
        if( subjectDirectory != null && node.isMaestro() ) {
            submitAll( node, subjectDirectory, waiter, searchTask );
            Object res[] = waiter.sync();
            node.setStopped();
            for( Object o: res ) {
                CompareImageTask.ImageMatches im = (CompareImageTask.ImageMatches) o;
                if( im.matches.size()>1 ) {
                    System.out.println( o.toString() );
                }
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
        File imageDir = null;
        String databases[] = new String[args.length-1];

        if( args.length<=1 ){
            System.err.println( "Usage: <image-directory> <database-directory>  ... <database-directory>" );
            System.err.println( "  Give a non-existent <image-directory> for a worker" );
            System.exit( 1 );
        }
        imageDir = new File( args[0] );
        System.arraycopy( args, 1, databases, 0, args.length-1 );
        if( !imageDir.isDirectory() ) {
            System.out.println( "Image directory " + imageDir + " does not exist" );
            imageDir = null;
        }
        System.out.println( "Running on platform " + Service.getPlatformVersion() + " input=" + imageDir + "; " + databases.length + " databases" );
        try {
            new FindNovaProgram().run( imageDir, databases );
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }
}
