package ibis.videoplayer;

import ibis.maestro.Node;
import ibis.maestro.Task;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * Compares an input image to the local images in this database.
 * 
 * @author Kees van Reeuwijk
 *
 */
class CompareImageTask implements Task {
    private static final long serialVersionUID = -4202576028676660015L;
    final File imageDirectory;
    
    CompareImageTask( File dir )
    {
	this.imageDirectory = dir;
    }

    static class ImageMatches implements Serializable
    {
	private static final long serialVersionUID = -6824625393338177074L;
	final UncompressedImage img;
        final File file;
	final ArrayList<File> matches = new ArrayList<File>();
	
	ImageMatches( UncompressedImage img, File file )
	{
	    this.img = img;
            this.file = file;
	}
	
	/**
	 * Returns a string representation of this match request.
	 * @return The string representation.
	 */
        @Override
        public String toString()
        {
            String res = "File " + file + " matches";

            if( matches.size() == 0 ) {
                res += " nothing";
            }
            else {
                for( File m: matches ) {
                    res += " " + m;
                }
            }
            return res;
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
    
    private void matchImages( File file, ImageMatches img )
    {
        if( file.isDirectory() ) {
            File files[] = file.listFiles();
            for( File f: files ) {
                matchImages( f, img );
            }
        }
        else {
            if( matchesImage( img.img, file ) ) {
                img.matches.add( file );
            }
        }
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

        matchImages( imageDirectory, img );
	return img;
    }

    /**
     * Returns true iff this job can be run on this node.
     * @return True iff we can access the image directory of this job.
     */
    @Override
    public boolean isSupported()
    {
	// TODO: a more extensive sanity check would be useful.
	return imageDirectory.isDirectory();
    }

}
