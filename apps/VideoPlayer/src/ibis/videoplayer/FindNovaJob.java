package ibis.videoplayer;

import ibis.maestro.Job;
import ibis.maestro.Node;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

/**
 * A given pair of images.
 * 
 * @author Kees van Reeuwijk
 *
 */
class FindNovaJob implements Job {
    private static final long serialVersionUID = -4202576028676660015L;

    static final int VERDICT_UNKNOWN = -1;
    static final int VERDICT_SAME = 0;
    static final int VERDICT_CHANGED = 1;

    static final class ImageIdentifier
    {
	final int generation;
	final int place;

	/**
	 * @param generation
	 * @param place
	 */
	ImageIdentifier(int generation, int place) {
	    this.generation = generation;
	    this.place = place;
	}

	@Override
	public String toString()
	{
	    return "[Image gen=" + generation + ",p=" + place + "]";
	}

	/**
	 * Returns the file for this image.
	 * @return
	 */
	public File buildFile() {
	    String nm = String.format( "gen%03d/img%05d.png", generation, place );
	    return new File( nm );
	}
    }

    FindNovaJob()
    {
    }

    static class ImageMatches implements Serializable
    {
	private static final long serialVersionUID = -6824625393338177074L;
	final ImageIdentifier before;
	final ImageIdentifier after;
	UncompressedImage beforeImage;
	UncompressedImage afterImage;
	int verdict = VERDICT_UNKNOWN;

	/**
	 * Constructs a new comparison request with the given two images.
	 * @param before The first image to compare.
	 * @param after The later image to compare.
	 */
	ImageMatches(ImageIdentifier before, ImageIdentifier after) {
	    this.before = before;
	    this.after = after;
	}

	@Override
	public String toString()
	{
	    String v = "unknown";
	    if( verdict == VERDICT_SAME ) {
		v = "same";
	    }
	    else if( verdict == VERDICT_CHANGED ) {
		v = "changed";
	    }
	    return "compare " + before + " to " + after + " verdict=" + v;
	}
    }

    private static boolean matchesImage( Image img, File f )
    {
	try {
	    Image other = UncompressedImage.loadPNG( f, 0 );
	    return img.equals( other );
	}
	catch( IOException x ) {
	    System.err.println( "Cannot read image file '" + f + "': " + x.getLocalizedMessage() );
	}
	return false;
    }

    private ImageMatches compareImages( ImageMatches img )
    {
	if( img.verdict == VERDICT_UNKNOWN ) {
	    // See if we can contribute.
	    if( img.beforeImage == null ) {
		img.beforeImage = loadImage( img.before );
	    }
	    if( img.afterImage == null ) {
		img.afterImage = loadImage( img.after );
	    }
	    if( img.beforeImage != null & img.afterImage != null ) {
		    boolean res = img.beforeImage.equals( img.afterImage );		
		    img.beforeImage = null;
		    img.afterImage = null;   // Don't burden the transmission with these images.
		    img.verdict = res?VERDICT_SAME:VERDICT_CHANGED;
	    }
	}
	return img;
    }

    /** Given an image identifier, try to load the image.
     * 
     * @param img The image to load.
     * @return The image, or <code>null</code> if it can not be loaded. 
     */
    private UncompressedImage loadImage( ImageIdentifier img )
    {
	File f = img.buildFile();
	if( !f.isFile() ) {
	    return null;
	}
	try {
	    return UncompressedImage.load( f, 0 );
	}
	catch( IOException x ) {
	    System.err.println( "Cannot read image file '" + f + "': " + x.getLocalizedMessage() );
	}
	return null;
    }

    /**
     * Run the matching process on a given image, and add any matches to its list.
     * @param input The image to compare with its found matches.
     * @param node The node this computation runs on.
     * @return The image and its matches, augmented with any matches on this site.
     */
    @Override
    public Object run( Object input, Node node )
    {
	ImageMatches img = (ImageMatches) input;

	return compareImages( img );
    }

    /**
     * Returns true iff this job can be run on this node.
     * @return True iff we can access the image directory of this job.
     */
    @Override
    public boolean isSupported()
    {
	return true;
    }

}
