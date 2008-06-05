package ibis.videoplayer;

import ibis.maestro.Job;
import ibis.maestro.Node;
import ibis.maestro.Service;
import ibis.maestro.Task;
import ibis.maestro.TaskWaiter;

import java.io.IOException;
import java.io.Serializable;

/**
 * Run some conversions on a directory full of images.
 * 
 * @author Kees van Reeuwijk
 *
 */
class BenchmarkProgram {
    static final int DVD_WIDTH = 720;
    static final int DVD_HEIGHT = 576;

    /** Empty class to send around when there is nothing to say. */
    final class Empty implements Serializable {
	private static final long serialVersionUID = 2;
    }

    private static final void ignore( Image img )
    {
	// Explicitly ignore an image.
    }

    // Do all the image processing steps in one go. Used as baseline.
    private final class ProcessFrameJob implements Job {
	private static final long serialVersionUID = -7976035811697720295L;

	/**
	 *
	 * @param in The input of this job.
	 * @param node The node we're running on.
	 * @return The fetched image.
	 */

	public Object run( Object in, Node node ) {
	    int frame = (Integer) in;

	    UncompressedImage img = RGB24Image.buildGradientImage( frame, DVD_WIDTH, DVD_HEIGHT, (byte) frame, (byte) (frame/10), (byte) (frame/100) );
	    img = img.scaleUp( 2 );
	    img = img.sharpen();
	    try {
		CompressedImage cimg = JpegCompressedImage.convert( img );
		return cimg;
	    }
	    catch( IOException e ) {
		System.err.println( "Cannot compress image: " + e.getLocalizedMessage() );
	    }
	    return null;
	}

	/**
	 * @param context The program context.
	 * @return True, because this job can run anywhere.
	 */
	@Override
	public boolean isSupported()
	{
	    return true;
	}

    }

    private final class GenerateFrameJob implements Job {
	private static final long serialVersionUID = -7976035811697720295L;

	/**
	 * @param in The input of this job.
	 * @param node The node we're running on.
	 * @return The fetched image.
	 */
	public Object run( Object in, Node node ) {
	    int frame = (Integer) in;
	    return RGB24Image.buildGradientImage( frame, DVD_WIDTH, DVD_HEIGHT, (byte) frame, (byte) (frame/10), (byte) (frame/100) );
	}

	/**
	 * @param context The program context.
	 * @return True, because this job can run anywhere.
	 */
	@Override
	public boolean isSupported()
	{
	    return true;
	}
    }

    private final class ScaleUpFrameJob implements Job
    {
	private static final long serialVersionUID = 5452987225377415308L;
	private final int factor;

	ScaleUpFrameJob( int factor )
	{
	    this.factor = factor;
	}

	/**Scale up one frame in a Maestro flow.
	 * 
	 * @param in The input of the conversion.
	 * @param node The node this process runs on.
	 * @return The scaled frame.
	 */
	@Override
	public Object run( Object in, Node node ) {
	    UncompressedImage img = (UncompressedImage) in;

	    return img.scaleUp( factor );
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

    private final class SharpenFrameJob implements Job
    {
	private static final long serialVersionUID = 54529872253774153L;

	/**Scale up one frame in a Maestro flow.
	 * 
	 * @param in The input of the conversion.
	 * @param node The node this process runs on.
	 * @return The scaled frame.
	 */
	@Override
	public Object run( Object in, Node node ) {
	    UncompressedImage img = (UncompressedImage) in;

	    return img.sharpen();
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

    private final class CompressFrameJob implements Job
    {
	private static final long serialVersionUID = 5452987225377415310L;

	/**
	 * Run a Jpeg conversion Maestro job.
	 * @param in The input of this job.
	 * @param node The node this job runs on.
	 * @return The result of the job.
	 */
	@Override
	public Object run( Object in, Node node ) {
	    UncompressedImage img = (UncompressedImage) in;

	    try {
		return JpegCompressedImage.convert( img );
	    } catch (IOException e) {
		System.err.println( "Cannot convert image to JPEG: " + e.getLocalizedMessage() );
		e.printStackTrace();
		return null;
	    }
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

    private final class IgnoreFrameJob implements Job
    {
	private static final long serialVersionUID = 54529872253774153L;

	/**Scale up one frame in a Maestro flow.
	 * 
	 * @param in The input of the conversion.
	 * @param node The node this process runs on.
	 * @return The scaled frame.
	 */
	@Override
	public Object run( Object in, Node node ) {
	    Image img = (Image) in;

	    return new Empty();
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

    @SuppressWarnings("synthetic-access")
    private void run( int frames, boolean goForMaestro ) throws Exception
    {
	Node node = new Node( goForMaestro );
	TaskWaiter waiter = new TaskWaiter();
	Task convertTask;
	if( true ) {
	    convertTask = node.createTask(
		    "benchmark",
		    new ProcessFrameJob(),
		    new IgnoreFrameJob()
	    );
	}
	else {
	    convertTask = node.createTask(
		    "benchmark",
		    new GenerateFrameJob(),
		    new ScaleUpFrameJob( 2 ),
		    new SharpenFrameJob(),
		    new CompressFrameJob(),
		    new IgnoreFrameJob()
	    );
	}

	System.out.println( "Node created" );
	long startTime = System.nanoTime();
	if( node.isMaestro() ) {
	    for( int frame=0; frame<frames; frame++ ){
		waiter.submit( convertTask, frame );
	    }
	    System.out.println( "Jobs submitted" );
	    waiter.sync();
	    System.out.println( "Jobs finished" );
	    node.setStopped();
	}
	node.waitToTerminate();
	long stopTime = System.nanoTime();
	System.out.println( "Duration of this run: " + Service.formatNanoseconds( stopTime-startTime ) );
    }

    /** The command-line interface of this program.
     * 
     * @param args The list of command-line parameters.
     */
    public static void main( String args[] )
    {
	boolean goForMaestro = false;
	int frames = 0;

	if( args.length>0 ){
	    frames = Integer.parseInt( args[0] );
	    goForMaestro = true;
	}
	//System.out.println( "Running on platform " + Service.getPlatformVersion() + " frames=" + frames + " goForMaestro=" + goForMaestro );
	try {
	    new BenchmarkProgram().run( frames, goForMaestro );
	}
	catch( Exception e ) {
	    e.printStackTrace( System.err );
	}
    }
}
