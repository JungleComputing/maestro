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

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;

abstract class UncompressedImage extends Image {

    UncompressedImage(int width, int height, int frameno) {
	super(width, height, frameno);
    }

    abstract IIOImage toIIOImage();

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
	WritableRaster raster = Raster.createBandedRaster( type, width, height, sm.getNumBands(), null );
	raster = image.copyData( raster );
	DataBuffer buffer = raster.getDataBuffer();
	if( buffer instanceof DataBufferUShort ){
	    DataBufferUShort sb = (DataBufferUShort) buffer;

	    short banks[][] = sb.getBankData();
	    return new RGB48Image( frameno, width, height, banks[0], banks[1], banks[2] );
	}
	if( buffer instanceof DataBufferByte ){
	    DataBufferByte sb = (DataBufferByte) buffer;

	    byte banks[][] = sb.getBankData();
	    return new RGB24Image( frameno, width, height, banks[0], banks[1], banks[2] );
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
	    byte r[] = new byte[width*height];
	    byte g[] = new byte[width*height];
	    byte b[] = new byte[width*height];
	    byte buf[] = new byte[3];

	    int ix = 0;
	    for( int h=0; h<height; h++ ) {
		for( int w=0; w<width; w++ ) {
		    int n = stream.read( buf );
		    r[ix] = buf[0];
		    g[ix] = buf[1];
		    b[ix] = buf[2];
		    ix++;
		}
	    }
	    stream.close();
	    return new RGB24Image( frameno, width, height, r, g, b ); 
	}
	short r[] = new short[width*height];
	short g[] = new short[width*height];
	short b[] = new short[width*height];
	byte buf[] = new byte[6];

	int ix = 0;
	for( int h=0; h<height; h++ ) {
	    for( int w=0; w<width; w++ ) {
		int n = stream.read( buf );
		r[ix] = buildShort( buf[0], buf[1] );
		g[ix] = buildShort( buf[2], buf[3] );
		b[ix] = buildShort( buf[4], buf[5]);
		ix++;
	    }
	}
	stream.close();
	return new RGB48Image( frameno, width, height, r, g, b ); 
    }

}