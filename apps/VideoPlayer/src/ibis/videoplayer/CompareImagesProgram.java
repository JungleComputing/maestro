package ibis.videoplayer;

import ibis.maestro.Job;
import ibis.maestro.Node;
import ibis.maestro.Task;
import ibis.maestro.TaskWaiter;

import java.io.File;
import java.io.IOException;

/**
 * Compare a directory full of images against distributed databases of images.
 * 
 * @author Kees van Reeuwijk
 *
 */
class CompareImagesProgram
{
    private void run( File subjectDirectory, String databaseList[] ) throws Exception
    {
        Node node = new Node( subjectDirectory != null );
        TaskWaiter waiter = new TaskWaiter();

        Job jobs[] = new Job[databaseList.length];
        int ix = 0;
        for( String db: databaseList ) {
            File dbf = new File( db );
            jobs[ix++] = new CompareImageJob( dbf );
        }
        Task searchTask =  node.createTask( "databaseSearch", jobs );
        System.out.println( "Node created" );
        if( subjectDirectory != null ) {
            submitAll( subjectDirectory, waiter, searchTask );
        }
        Object res[] = waiter.sync();
        for( Object o: res ) {
            CompareImageJob.ImageMatches im = (CompareImageJob.ImageMatches) o;
            if( im.matches.size()>1 ) {
                System.out.println( o.toString() );
            }
        }
        node.setStopped();
        node.waitToTerminate();
    }

    /**
     * @param file The file or directory to submit.
     * @param waiter The waiter thread that will wait for the return of the results.
     * @param searchTask The task to submit the files to.
     */
    private void submitAll( File file, TaskWaiter waiter, Task searchTask )
    {
        if( file.isDirectory() ) {
            File files[] = file.listFiles();
            for( File f: files ) {
                submitAll( f, waiter, searchTask );
            }
        }
        else {
            try {
                UncompressedImage img = UncompressedImage.load( file, 0 );
                CompareImageJob.ImageMatches im = new CompareImageJob.ImageMatches( img, file );
                waiter.submit( searchTask, im );
            }
            catch( IOException e ) {
                System.err.println( "Cannot load image '" + file + "': " + e.getLocalizedMessage() );
            }
        }
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
            new CompareImagesProgram().run( imageDir, databases );
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }
}
