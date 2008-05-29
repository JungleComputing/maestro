package ibis.videoplayer;

import java.awt.Transparency;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.PixelInterleavedSampleModel;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
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

    static final int BANDS = 3;

    /** The data of the image.
     * We currently assume that there are three bands in there, each
     * with one unsigned byte of value, with the meaning Red, Green, Blue.
     */
    final byte data[];

    RGB24Image( int frameno, int width, int height, byte[] data )
    {
        super( width, height, frameno );
        this.data = data;
    }

    /**
     * Returns a string representation of this frame.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "frame " + frameno + " RGB24 " + width + "x" + height + "; " + BANDS + " channels";
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

    @Override
    UncompressedImage scaleDown( int factor )
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
        int weight = factor*factor;
        int wt2 = weight/2;  // Used for rounding.
        int swidth = width/factor;
        int sheight = height/factor;
        byte res[] = new byte[swidth*sheight*BANDS];

        int ix = 0;
        for( int y=0; y<sheight; y++ ) {
            for( int x=0; x<swidth; x++ ){
                int redValues = 0; // The sum of the red values we're going to average.
                int greenValues = 0;
                int blueValues = 0;

                // Compute the offset in the channel for the first row of pixels.
                // 
                int offset = x*factor+y*factor*width*BANDS;
                for( int ypix=0; ypix<factor; ypix++ ) {
                    for( int xpix=0; xpix<factor; xpix += BANDS ) {
                        int vr = data[offset+xpix];
                        int vg = data[offset+xpix+1];
                        int vb = data[offset+xpix+2];

                        // Convert to unsigned and add to the average.
                        redValues += (0xFFFF & vr);
                        greenValues += (0xFFFF & vg);
                        blueValues += (0xFFFF & vb);
                    }
                    offset += width*BANDS;
                }
                // Store rounded values.
                res[ix++] = (byte) ((redValues+wt2)/weight);
                res[ix++] = (byte) ((greenValues+wt2)/weight);
                res[ix++] = (byte) ((blueValues+wt2)/weight);
            }
        }
        if( Settings.traceScaler ){
            System.out.println( "Scaling " + this + " by factor " + factor );
        }
        return new RGB24Image( frameno, width/factor, height/factor, res );
    }

    @Override
    Image colourCorrect( double frr, double frg, double frb, double fgr, double fgg, double fgb, double fbr, double fbg, double fbb )
    {
	byte res[] = new byte[width*height*BANDS];

	// Apply the color correction matrix 
        // We blindly assume r,g, and b, have the same length.
        for( int i=0; i<data.length; i += BANDS ) {
            double vr = frr*data[i] + frg*data[i+1] + frb*data[i+2];
            double vg = fgr*data[i] + fgg*data[i+1] + fgb*data[i+2];
            double vb = fbr*data[i] + fbg*data[i+1] + fbb*data[i+2];

            res[i] = (byte) vr;
            res[i+1] = (byte) vg;
            res[i+2] = (byte) vb;
        }
        if( Settings.traceActions ) {
            System.out.println( "Color-corrected " + this );
        }
        return new RGB24Image( frameno, width, height, res );
    }

    /**
     * Returns a JPeg compressed version of this image.
     * @throws IOException Thrown if the conversion causes an I/O error
     */
    JpegCompressedImage toJpegImage() throws IOException
    {
        DataBuffer buffer = new DataBufferByte( data, data.length );
        int offsets[] = new int[] { 0, 1, 2 };
        SampleModel sampleModel = new PixelInterleavedSampleModel( buffer.getDataType(), width, height, BANDS, BANDS*width, offsets );
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
        // Since the format of PPM pixels is the same, we can just write
        // all the bytes directly.
        stream.write( data );
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
                stream.format( "%02x %02x %02x\n", data[ix++], data[ix++], data[ix++] );
            }
            stream.println();
        }
        stream.close();
    }

    static RGB24Image buildConstantImage( int frameno, int width, int height, int vr, int vg, int vb )
    {
        byte data[] = new byte[width*height*BANDS];

        int ix = 0;
        for( int h=0; h<height; h++ ) {
            for( int w=0; w<width; w++ ) {
        	data[ix++] = (byte) vr;
        	data[ix++] = (byte) vg;
        	data[ix++] = (byte) vb;
            }
        }
        return new RGB24Image( frameno, width, height, data );
    }

    static RGB24Image buildGradientImage( int frameno, int width, int height )
    {
        byte data[] = new byte[width*height*BANDS];
        int ix = 0;
        byte vg = 0;

        for( int h=0; h<height; h++ ) {
            byte vr = 0;
            for( int w=0; w<width; w++ ) {
                data[ix++] = vr;
                data[ix++] = vg;
                data[ix++] = 127;
                vr++;
            }
            vg++;
        }
        return new RGB24Image( frameno, width, height, data );
    }

    static RGB24Image convert( Image img )
    {
        if( img instanceof RGB24Image ) {
            // Now this is easy.
            return (RGB24Image) img;
        }
        if( img instanceof RGB48Image ){
            RGB48Image img48 = (RGB48Image) img;

            return img48.buildRGB24Image();
        }
        System.err.println( "Don't know how to convert a " + img.getClass() + " to a RGB24 image" );
        return null;
    }
}