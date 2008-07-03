package ibis.videoplayer;

import ibis.maestro.CompletionListener;
import ibis.maestro.Job;
import ibis.maestro.JobList;
import ibis.maestro.Node;
import ibis.maestro.Service;
import ibis.maestro.Task;

import java.io.File;
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

    static final File outputDir = new File("output");

    private static class Listener implements CompletionListener
    {
	private final LabelTracker labelTracker = new LabelTracker();
	private boolean sentFinal = false;

	Listener()
	{
	}

	/** Handle the completion of task 'j': the result is 'result'.
	 * @param id The task that was completed.
	 * @param result The result of the task.
	 */
	@Override
	public void jobCompleted( Node node, Object id, Object result )
	{
	    if( !(id instanceof LabelTracker.Label)) {
		System.err.println( "Internal error: Object id is not a tracker label: " + id );
		System.exit( 1 );
	    }
	    labelTracker.returnLabel( (LabelTracker.Label) id );
	    if( sentFinal && labelTracker.allAreReturned() ) {
		System.out.println( "I got all task results back; stopping test program" );
		node.setStopped();
	    }
	}

	Object getLabel() {
	    return labelTracker.nextLabel();
	}

	void setFinished() {
	    sentFinal = true;	    
	}
    }

    /** Empty class to send around when there is nothing to say. */
    static final class Empty implements Serializable {
	private static final long serialVersionUID = 2;
    }

    // Do all the image processing steps in one go. Used as baseline.
    private static final class ProcessFrameTask implements Task {
	private static final long serialVersionUID = -7976035811697720295L;
	final boolean slowScale;
	final boolean slowSharpen;
	final File saveDir;

	ProcessFrameTask( final boolean slowScale, final boolean slowSharpen, final File saveDir )
	{
	    this.slowScale = slowScale;
	    this.slowSharpen = slowSharpen;
	    this.saveDir = saveDir;
	}

	/**
	 *
	 * @param in The input of this job.
	 * @param node The node we're running on.
	 * @return The fetched image.
	 */
	public Object run( Object in, Node node ) {
	    int frame = (Integer) in;

	    UncompressedImage img = RGB24Image.buildGradientImage( frame, DVD_WIDTH, DVD_HEIGHT, (byte) frame, (byte) (frame/10), (byte) (frame/100) );
	    if( slowScale ) {
		img.scaleUp(  2 );
	    }
	    img = img.scaleUp( 2 );
	    if( slowSharpen ) {
		img.sharpen();
	    }
	    img = img.sharpen();
	    try {
		CompressedImage cimg = JpegCompressedImage.convert( img );
		if( saveDir != null ) {
		    if( !saveDir.isDirectory() ) {
			saveDir.mkdir();
		    }
		    File f = new File( saveDir, String.format( "frame%05d.jpg", img.frameno ) );
		    cimg.write( f );
		}
		return new Empty();
	    }
	    catch( IOException e ) {
		System.err.println( "Cannot compress image: " + e.getLocalizedMessage() );
	    }
	    return null;
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

    private static final class GenerateFrameTask implements Task {
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
	 * @return True, because this job can run anywhere.
	 */
	@Override
	public boolean isSupported()
	{
	    return true;
	}
    }

    private static final class ScaleUpFrameTask implements Task
    {
	private static final long serialVersionUID = 5452987225377415308L;
	private final int factor;
	private final boolean slow;
	private final boolean allowed;

	ScaleUpFrameTask( int factor, boolean slow, boolean allowed )
	{
	    this.factor = factor;

	    this.slow = slow;
	    this.allowed = allowed;
	    if( allowed ) {
		if( slow ){
		    System.out.println( "Using slow upscaling" );
		}
	    }
	    else {
		System.out.println( "Upscaling not allowed" );
	    }
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

	    if( slow ) {
		img.scaleUp( factor );
	    }
	    Object res = img.scaleUp( factor );
	    return res;
	}

	/**
	 * @return True iff the allowed flag is set for this class.
	 */
	@Override
	public boolean isSupported()
	{
	    return allowed;
	}
    }

    private static final class SharpenFrameTask implements Task
    {
	private static final long serialVersionUID = 54529872253774153L;
	private boolean slow;
	private boolean allowed;

	SharpenFrameTask( boolean slow, boolean allowed )
	{
	    this.slow = slow;
	    this.allowed = allowed;
	    if( allowed ) {
		if( slow ){
		    System.out.println( "Using slow sharpen" );
		}
	    }
	    else {
		System.out.println( "Sharpen not allowed" );
	    }
	}

	/** Sharpen one frame in a Maestro flow.
	 * 
	 * @param in The input of the conversion.
	 * @param node The node this process runs on.
	 * @return The scaled frame.
	 */
	@Override
	public Object run( Object in, Node node ) {
	    UncompressedImage img = (UncompressedImage) in;

	    if( slow ) {
		img.sharpen();
	    }
	    return img.sharpen();
	}

	/**
	 * @return True iff allowed is set.
	 */
	@Override
	public boolean isSupported()
	{
	    return allowed;
	}
    }

    private static final class CompressFrameTask implements Task
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

    private static final class SaveFrameTask implements Task
    {
	private static final long serialVersionUID = 54529872253774153L;
	private final File saveDir;

	SaveFrameTask( File saveDir )
	{
	    this.saveDir = saveDir;
	}

	/** Optionally save one frame in a Maestro flow.
	 * This job is placed at the end of a benchmark flow, and normally
	 * just ignores the received image. Optionally it can store the
	 * image for debugging purposes.
	 * 
	 * @param in The input of the conversion.
	 * @param node The node this process runs on.
	 * @return The scaled frame.
	 */
	@Override
	public Object run( Object in, Node node ) {
	    Image img = (Image) in;

	    if( saveDir != null ) {
		if( !saveDir.isDirectory() ) {
		    saveDir.mkdir();
		}
		File f = new File( saveDir, String.format( "frame%05d.jpg", img.frameno ) );
		try {
		    img.write( f );
		} catch (IOException e) {
		    // TODO: Auto-generated catch block
		    e.printStackTrace();
		}
	    }
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

    private boolean goForMaestro = false;
    private int frames = 0;
    private boolean saveFrames = false;
    private boolean slowSharpen = false;
    private boolean slowScale = false;
    private boolean allowSharpen = true;
    private boolean allowScale = true;
    private boolean oneJob = false;

    private static void printUsage()
    {

	System.out.println( "Usage: [<flags>] <frame-count>" );
    }

    private boolean parseArgs( String args[] )
    {
	String frameCount = null;

	for( String arg: args ) {
	    if( arg.equalsIgnoreCase( "-h") || arg.equalsIgnoreCase( "-help" ) ){
		printUsage();
		return false;
	    }
	    if( arg.equalsIgnoreCase( "-save" ) ) {
		saveFrames = true;
	    }
	    else if( arg.equalsIgnoreCase( "-onejob" ) ) {
		oneJob = true;
	    }
	    else if( arg.equalsIgnoreCase( "-slowsharpen" ) ) {
		slowSharpen = true;
	    }
	    else if( arg.equalsIgnoreCase( "-slowscale" ) ) {
		slowScale = true;
	    }
	    else {
		if( frameCount != null ){
		    System.err.println( "Duplicate frame count. Was: [" + frameCount + "] new: [" + arg + "]" );
		    return false;
		}
		frameCount = arg;
	    }
	}
	if( frameCount != null ){
	    frames = Integer.parseInt( frameCount );
	    goForMaestro = true;
	}
	return true;
    }

    private static void removeDirectory( File f )
    {
	if( f == null ) {
	    return;
	}
	if( f.isFile() ) {
	    f.delete();
	}
	else if( f.isDirectory() ) {
	    for( File e: f.listFiles() ) {
		removeDirectory( e );
	    }
	    f.delete();
	}
    }

    @SuppressWarnings("synthetic-access")
    private void run( String args[] ) throws Exception
    {
	if( !parseArgs( args ) ){
	    System.err.println( "Parsing command line failed. Goodbye!" );
	    System.exit( 1 );
	}
	System.out.println( "frames=" + frames + " goForMaestro=" + goForMaestro + " saveFrames=" + saveFrames + " oneJob=" + oneJob + " slowSharpen=" + slowSharpen + " slowScale=" + slowScale  );
	JobList tasks = new JobList();
	Job convertTask;
	Listener listener = new Listener();
	File dir = saveFrames?outputDir:null;
	if( oneJob ) {
	    System.out.println( "One-job benchmark" );
	    convertTask = tasks.createJob(
		    "benchmark",
		    new ProcessFrameTask( slowScale, slowSharpen, dir )
	    );
	}
	else {
	    if( saveFrames ){
		convertTask = tasks.createJob(
			"benchmark",
			new GenerateFrameTask(),
			new ScaleUpFrameTask( 2, slowScale, allowScale ),
			new SharpenFrameTask( slowSharpen, allowSharpen ),
			new CompressFrameTask(),
			new SaveFrameTask( dir )
		);
	    }
	    else {
		convertTask = tasks.createJob(
			"benchmark",
			new GenerateFrameTask(),
			new ScaleUpFrameTask( 2, slowScale, allowScale ),
			new SharpenFrameTask( slowSharpen, allowSharpen ),
			new CompressFrameTask(),
			new SaveFrameTask( dir )
		);
	    }
	}
	Node node = new Node( tasks, goForMaestro );

	removeDirectory( dir );

	System.out.println( "Node created" );
	long startTime = System.nanoTime();
	if( node.isMaestro() ) {
	    for( int frame=0; frame<frames; frame++ ){
		Object label = listener.getLabel();
		convertTask.submit( node, frame, label, listener );
	    }
	    listener.setFinished();
	    System.out.println( "Jobs submitted" );
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
	try {
	    new BenchmarkProgram().run( args );
	}
	catch( Exception e ) {
	    System.err.println( "main() caught an exception" );
	    e.printStackTrace( System.err );
	}
    }
}
