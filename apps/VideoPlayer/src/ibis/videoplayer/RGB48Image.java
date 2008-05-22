package ibis.videoplayer;

import java.awt.image.BandedSampleModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferUShort;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.io.File;

import javax.imageio.IIOImage;
import javax.imageio.metadata.IIOMetadata;

/**
 * A video frame.
 * 
 * @author Kees van Reeuwijk
 */
class RGB48Image extends UncompressedImage {
    private static final long serialVersionUID = 8797700803728846092L;
    /**
     * The channels of the image. Each channel stores its values consecutively in one
     * large array row by row from top to bottom.
     */
    final short r[];
    final short g[];
    final short b[];

    RGB48Image( int frameno, int width, int height, short r[], short g[], short b[] )
    {
        super( width, height, frameno );
	this.r = r;
	this.g = g;
	this.b = b;
    }
    
    /**
     * Returns a string representation of this frame.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
	return "frame " + frameno + " " + width + "x" + height;
    }
    
    /**
     * Make sure the given dimension is a multiple of the given factor.
     * If not, use the given name in any error message we generate.
     * @param dim The dimension to check.
     * @param name The name of the dimension.
     * @param factor The factor that should occur in the dimension
     * @return True iff every checks out ok.
     */
    private static boolean checkFactor( int dim, String name, int factor )
    {
	if( ((dim/factor)*factor) != dim ) {
	    System.err.println( "The " + name + " of this image (" + dim + ") is not a multiple of " + factor );
	    return false;
	}
	return true;
    }

    /**
     * Given a color channel and the dimensions of the image it belongs to,
     * and a scale-down factor to apply, return a scaled version of the channel.
     * We blindly assume that the dimensions of the channel are correct
     * for the image, and that the dimensions actually allow exact scaling
     * down with this factor.
     * 
     * The scaling down uses a pretty dumb algorithm: unweighted averaging
     * over all pixels in the original.
     * 
     * @param channel The color channel.
     * @param w The width of the image.
     * @param h The height of the image.
     * @param factor The scale-down factor to apply.
     * @return The scaled-down image.
     */
    private short[] scaleChannel( short channel[], int w, int h, int factor )
    {
        int weight = factor*factor;
        int wt2 = weight/2;  // Used for rounding.
        int swidth = w/factor;
        int sheight = h/factor;
        short res[] = new short[swidth*sheight];
        
        int ix = 0;
        for( int x=0; x<swidth; x++ ){
            for( int y=0; y<sheight; y++ ) {
        	int values = 0; // The sum of the values we're going to average.

        	// Compute the offset in the channel for the first row of pixels.
        	// 
        	int offset = y*factor*w;
        	for( int ypix=0; ypix<factor; ypix++ ) {
        	    for( int xpix=0; xpix<factor; xpix++ ) {
        		int v = channel[offset+xpix];
        		values += (0xFFFF & v); // Convert to unsigned and add to the average.
        	    }
        	    offset += w;
        	}
        	res[ix++] = (short) ((values+wt2)/weight); // Store rounded value.
            }
        }
        return res;
    }

    @Override
    Image scaleDown( int factor )
    {
        if( Settings.traceScaler ){
            System.out.println( "Scaling frame " + frameno );
        }
        if( !checkFactor( width, "width", factor ) ) {
            return null;
        }
        if( !checkFactor( height, "height", factor ) ) {
            return null;
        }
        short outr[] = scaleChannel( r, width, height, factor );
        short outg[] = scaleChannel( g, width, height, factor );
        short outb[] = scaleChannel( b, width, height, factor );
        if( Settings.traceScaler ){
            System.out.println( "Scaling " + this + " by factor " + factor );
        }
        return new RGB48Image( frameno, width/2, height/2, outr, outg, outb );
    }


    Image colorCorrect( double frr, double frg, double frb, double fgr, double fgg, double fgb, double fbr, double fbg, double fbb )
    {
        // Apply the color correction matrix 
        // We blindly assume r,g, and b, have the same length.
        for( int i=0; i<r.length; i++ ) {
            double vr = frr*r[i] + frg*g[i] + frb*b[i];
            double vg = fgr*r[i] + fgg*g[i] + fgb*b[i];
            double vb = fbr*r[i] + fbg*g[i] + fbb*b[i];
            
            r[i] = (short) vr;
            g[i] = (short) vg;
            g[i] = (short) vb;
        }
        if( Settings.traceActions ) {
            System.out.println( "Color-corrected " + this );
        }
        return new RGB48Image( frameno, width, height, r, g, b );
    }

    /**
     * Returns an IIOImage of this image.
     */
    @Override
    IIOImage toIIOImage()
    {
        short buffers[][] = new short[][] { r, g, b };
        DataBuffer buffer = new DataBufferUShort( buffers, r.length );
        SampleModel sampleModel = new BandedSampleModel( buffer.getDataType(), width, height, 3 );
        Raster raster = Raster.createRaster( sampleModel, buffer, null );
        IIOMetadata metadata = null;
        IIOImage image = new IIOImage( raster, null, metadata ); 
        return image;
    }
    
    /** Writes this image to the given file. */
    @Override
    void write( File f )
    {
    }
}