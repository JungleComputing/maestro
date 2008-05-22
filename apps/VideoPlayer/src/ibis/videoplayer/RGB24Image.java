package ibis.videoplayer;

import java.awt.image.BandedSampleModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import javax.imageio.IIOImage;
import javax.imageio.metadata.IIOMetadata;

/**
 * A video frame.
 * 
 * @author Kees van Reeuwijk
 */
class RGB24Image extends UncompressedImage {
    private static final long serialVersionUID = 8797700803728846092L;
    /**
     * The channels of the image. Each channel stores its values consecutively in one
     * large array row by row from top to bottom.
     */
    final byte r[];
    final byte g[];
    final byte b[];

    RGB24Image( int frameno, int width, int height, byte[] r, byte[] g, byte[] b )
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
	return "frame " + frameno + " RGB24 " + width + "x" + height;
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
    private byte[] scaleChannel( byte channel[], int w, int h, int factor )
    {
        int weight = factor*factor;
        int wt2 = weight/2;  // Used for rounding.
        int swidth = w/factor;
        int sheight = h/factor;
        byte res[] = new byte[swidth*sheight];
        
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
        	res[ix++] = (byte) ((values+wt2)/weight); // Store rounded value.
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
        byte outr[] = scaleChannel( r, width, height, factor );
        byte outg[] = scaleChannel( g, width, height, factor );
        byte outb[] = scaleChannel( b, width, height, factor );
        if( Settings.traceScaler ){
            System.out.println( "Scaling " + this + " by factor " + factor );
        }
        return new RGB24Image( frameno, width/2, height/2, outr, outg, outb );
    }


    Image colorCorrect( double frr, double frg, double frb, double fgr, double fgg, double fgb, double fbr, double fbg, double fbb )
    {
        // Apply the color correction matrix 
        // We blindly assume r,g, and b, have the same length.
        for( int i=0; i<r.length; i++ ) {
            double vr = frr*r[i] + frg*g[i] + frb*b[i];
            double vg = fgr*r[i] + fgg*g[i] + fgb*b[i];
            double vb = fbr*r[i] + fbg*g[i] + fbb*b[i];
            
            r[i] = (byte) vr;
            g[i] = (byte) vg;
            g[i] = (byte) vb;
        }
        if( Settings.traceActions ) {
            System.out.println( "Color-corrected " + this );
        }
        return new RGB24Image( frameno, width, height, r, g, b );
    }

    /**
     * Returns an IIOImage of this image.
     */
    @Override
    IIOImage toIIOImage()
    {
        byte buffers[][] = new byte[][] { r, g, b };
        DataBuffer buffer = new DataBufferByte( buffers, r.length );
        SampleModel sampleModel = new BandedSampleModel( buffer.getDataType(), width, height, 3 );
        Raster raster = Raster.createRaster( sampleModel, buffer, null );
        IIOMetadata metadata = null;
        IIOImage image = new IIOImage( raster, null, metadata ); 
        return image;
    }

    /**
     * Writes this image to the given file.
     * The file will be written in ppm format.
     * @param f The file to write to.
     */
    @Override
    void write( File f ) throws IOException
    {
        FileOutputStream stream = new FileOutputStream( f );
        String header = "P6\n" + width + ' ' + height + "\n255\n";
        stream.write( header.getBytes() );
        int ix = 0;
        for( int h=0; h<height; h++ ) {
            for( int w=0; w<width; w++ ) {
        	stream.write( r[ix] );
        	stream.write( g[ix] );
        	stream.write( b[ix] );
            }
        }
        stream.close();
    }
    
    private static byte[] fillChannel( int width, int height, int val )
    {
        byte res[] = new byte[width*height];

        Arrays.fill( res, (byte) val );
        return res;
    }

    static RGB24Image fillImage( int frameno, int width, int height, int vr, int vg, int vb )
    {
        byte r[] = fillChannel( width, height, vr );
        byte g[] = fillChannel( width, height, vg );
        byte b[] = fillChannel( width, height, vb );
        return new RGB24Image( frameno, width, height, r, g, b );
    }

    static UncompressedImage convert( Image img )
    {
        if( img instanceof RGB48Image ){
            RGB48Image img48 = (RGB48Image) img;
            
            return img48.buildRGB24Image();
        }
        System.err.println( "Don't know how to convert a " + img.getClass() + " to a RGB24 image" );
        return null;
    }

}