package ibis.videoplayer;

import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.DataBufferUShort;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.imageio.ImageIO;

abstract class UncompressedImage extends Image {

    UncompressedImage(int width, int height, int frameno) {
	super(width, height, frameno);
    }
    /**
     * Returns a new image with each of the dimensions reduced by the
     * given factor.
     * @param factor The scale factor.
     * @return The scaled image.
     */
    @Override
    abstract UncompressedImage scaleDown( int factor );
    abstract Image colorCorrect( double frr, double frg, double frb, double fgr, double fgg, double fgb, double fbr, double fbg, double fbb );

    /**
     * Given a file that we know is a PNG file, load it, and return
     * and uncompressed image for it.
     * @param f The file to load.
     * @return The image of that file.
     * @throws IOException Thrown if the file cannot be read.
     */
    static UncompressedImage loadPNG( File f, int frameno ) throws IOException
    {
	BufferedImage image = ImageIO.read( f );
	SampleModel sm = image.getSampleModel();
	int type = sm.getDataType();
	final int width = sm.getWidth();
	final int height = sm.getHeight();
	WritableRaster raster = Raster.createInterleavedRaster( type, width, height, sm.getNumBands(), null );
	raster = image.copyData( raster );
	DataBuffer buffer = raster.getDataBuffer();
	if( buffer instanceof DataBufferUShort ){
	    DataBufferUShort sb = (DataBufferUShort) buffer;

	    short banks[][] = sb.getBankData();
	    return new RGB48Image( frameno, width, height, banks[0] );
	}
	if( buffer instanceof DataBufferByte ){
	    DataBufferByte sb = (DataBufferByte) buffer;

	    byte banks[][] = sb.getBankData();
	    return new RGB24Image( frameno, width, height, banks[0] );
	}
	System.err.println( "Don't know how to handle data buffer type " + buffer.getClass() );
	return null;
    }

    private static int readNumber( FileInputStream stream ) throws IOException
    {
	int val = 0;

	while( true ) {
	    int c = stream.read();
	    if( c == ' ' || c == '\n' ) {
		return val;
	    }
	    if( c>='0' && c <= '9' ) {
		val = 10*val + (c-'0');
	    }
	    else {
		System.err.println( "Unexpected character '" + (char) c + "' in PPM header" );
		return val;
	    }
	}
    }

    /**
     * Given two byte values, build a short.
     * @param high The high value of the short.
     * @param low The low value of the short.
     * @return The built short value.
     */
    private static short buildShort( int high, int low )
    {
	return (short) ((low & 0xFF) + (high & 0xFF)*256);
    }

    /**
     * Keep reading bytes from the given stream until the entire buffer has been filled.
     * We might have to give up, however, if end of file is reached. In that case <code>false</code>
     * is returned, and the buffer is necessarily only partially filled. Since this is an error
     * situation, we don't bother to pass on how much of the buffer was filled.
     * @param stream The stream to read from.
     * @param buffer The buffer to fill.
     * @return True iff we managed to fill the entire buffer.
     * @throws IOException Thrown if there is a read error.
     */
    private static boolean readBuffer( InputStream stream, byte buffer[] ) throws IOException
    {
	int offset = 0;

	while( true ) {
	    int n = stream.read( buffer, offset, buffer.length-offset );
	    if( n<0 ) {
		return false;
	    }
	    offset += n;
	    if( offset>=buffer.length ) {
		break;
	    }
	}
	return true;
    }

    /**
     * Given a file that we know is a PPM file, load it, and return
     * and uncompressed image for it.
     * @param f The file to load.
     * @return The image of that file.
     * @throws IOException Thrown if the file cannot be read.
     */
    static UncompressedImage loadPPM( File f, int frameno ) throws IOException
    {
	FileInputStream stream = new FileInputStream( f );
	int c1 = stream.read();
	int c2 = stream.read();
	int c3 = stream.read();
	if( c1 != 'P' || c2 != '6' || c3 != '\n' ) {
	    System.err.println( "Bad magic for a PPM file" );
	    stream.close();
	    return null;
	}
	int width = readNumber( stream );
	int height = readNumber( stream );
	int maxVal = readNumber( stream );
	if( width == 0 ) {
	    System.err.println( "Zero width in PPM file " + f );
	    stream.close();
	    return null;
	}
	if( height == 0 ) {
	    System.err.println( "Zero height in PPM file " + f );
	    stream.close();
	    return null;
	}
	if( maxVal != 255 && maxVal != 65535 ) {
	    System.err.println( "Unsupported maxVal " + maxVal + " in PPM file " + f );
	    stream.close();
	    return null;
	}
	if( maxVal == 255 ) {
	    byte data[] = new byte[width*height*RGB24Image.BANDS];

	    if( !readBuffer( stream, data ) ) {
		System.err.println( "Image data ended prematurely" );
	    }
	    stream.close();
	    return new RGB24Image( frameno, width, height, data ); 
	}
	short data[] = new short[width*height*RGB48Image.BANDS];
	byte buf[] = new byte[2*RGB48Image.BANDS*width];

	int ix = 0;
	for( int h=0; h<height; h++ ) {
	    if( !readBuffer( stream, buf ) ) {
		System.err.println( "Image data ended prematurely" );
		break;
	    }
	    int bufix = 0;
	    for( int w=0; w<width*RGB48Image.BANDS; w++ ) {
		data[ix++] = buildShort( buf[bufix++], buf[bufix++] );
	    }
	}
	stream.close();
	return new RGB48Image( frameno, width, height, data ); 
    }

}