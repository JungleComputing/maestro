package ibis.videoplayer;

import ibis.maestro.Job;
import ibis.maestro.Node;
import ibis.maestro.Task;
import ibis.maestro.TaskWaiter;

import java.io.File;

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
	    File files[] = subjectDirectory.listFiles();
	    System.out.println( "I am maestro; converting " + files.length + " images" );
	    for( File f: files ) {
		waiter.submit( searchTask, f );
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
