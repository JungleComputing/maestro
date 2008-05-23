package ibis.videoplayer;

import java.awt.Transparency;
import java.awt.color.ColorSpace;
import java.awt.image.BandedSampleModel;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;

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
    private byte[] scaleDownChannel( byte channel[], int w, int h, int factor )
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
        byte outr[] = scaleDownChannel( r, width, height, factor );
        byte outg[] = scaleDownChannel( g, width, height, factor );
        byte outb[] = scaleDownChannel( b, width, height, factor );
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
     * Returns a JPeg compressed version of this image.
     * @throws IOException Thrown if the conversion causes an I/O error
     */
    JpegCompressedImage toJpegImage() throws IOException
    {
        byte buffers[][] = new byte[][] { r, g, b };
        DataBuffer buffer = new DataBufferByte( buffers, r.length );
        SampleModel sampleModel = new BandedSampleModel( buffer.getDataType(), width, height, 3 );
        int bits[] = new int[] { 8, 8, 8 };
        ColorModel colorModel = new ComponentColorModel( ColorSpace.getInstance( ColorSpace.CS_sRGB ), bits, false, false, Transparency.OPAQUE, buffer.getDataType() );
        BufferedImage img = new BufferedImage( colorModel, Raster.createWritableRaster( sampleModel, buffer, null ), false, null );
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        ImageIO.setUseCache( false );
        ImageOutputStream output = ImageIO.createImageOutputStream(out);
        Iterator<?> writers = ImageIO.getImageWritersByFormatName( "jpg" );

        if( writers == null || !writers.hasNext() ){
            throw new RuntimeException( "No jpeg writers!" );
        }
        ImageWriter writer = (ImageWriter) writers.next();
        writer.setOutput( output );
        writer.write( img );
        writer.dispose();

        byte [] tmp = out.toByteArray();

      //  System.out.println("Compression " + image.width + "x" + image.height + " from " + image.getSize() + " to " + tmp.length + " bytes.");
        
        return new JpegCompressedImage( width, height, frameno, tmp ); 
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
        byte buffer[] = new byte[3*width];
        for( int h=0; h<height; h++ ) {
            int bufix = 0;
            for( int w=0; w<width; w++ ) {
                buffer[bufix++] = r[ix];
                buffer[bufix++] = g[ix];
                buffer[bufix++] = b[ix];
                ix++;
            }
            stream.write( buffer );
        }
        stream.close();
    }

    /** Prints a text dump of this image to the given file. 
     * @param f The file to write to.
     * @throws IOException Thrown if the image cannot be written.
     */
    @Override
    void print( File f ) throws IOException
    {
        PrintStream stream = new PrintStream( new FileOutputStream( f ) );
        stream.println( "RGB24 " + width + "x" + height + " frame " + frameno );
        int ix = 0;
        for( int h=0; h<height; h++ ) {
            for( int w=0; w<width; w++ ) {
                stream.format( "%02x %02x %02x\n", r[ix], g[ix], b[ix] );
            }
            stream.println();
        }
        stream.close();
    }

    private static byte[] fillChannel( int width, int height, int val )
    {
        byte res[] = new byte[width*height];

        Arrays.fill( res, (byte) val );
        return res;
    }

    static RGB24Image buildConstantImage( int frameno, int width, int height, int vr, int vg, int vb )
    {
        byte r[] = fillChannel( width, height, vr );
        byte g[] = fillChannel( width, height, vg );
        byte b[] = fillChannel( width, height, vb );
        return new RGB24Image( frameno, width, height, r, g, b );
    }

    static RGB24Image buildGradientImage( int frameno, int width, int height )
    {
        byte r[] = new byte[width*height];
        byte g[] = new byte[width*height];
        byte b[] = new byte[width*height];
        int ix = 0;
        byte vg = 0;

        for( int h=0; h<height; h++ ) {
            byte vr = 0;
            for( int w=0; w<width; w++ ) {
                r[ix] = vr;
                g[ix] = vg;
                b[ix] = 127;
                ix++;
                vr++;
            }
            vg++;
        }
        return new RGB24Image( frameno, width, height, r, g, b );
    }

    static RGB24Image convert( Image img )
    {
        if( img instanceof RGB48Image ){
            RGB48Image img48 = (RGB48Image) img;

            return img48.buildRGB24Image();
        }
        System.err.println( "Don't know how to convert a " + img.getClass() + " to a RGB24 image" );
        return null;
    }

}