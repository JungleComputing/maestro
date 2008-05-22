package ibis.videoplayer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

class JpegCompressedImage extends CompressedImage {

    /** 
     * Generated
     */
    private static final long serialVersionUID = 2943319117224074964L;

    private final byte [] data;

    JpegCompressedImage( int width, int height, int frameno, byte [] data )
    {
	super( width, height, frameno );
	this.data = data;
    }

    /**
     * Writes the image to the given file.
     * @param f The file to write to.
     */
    @Override
    void write( File f ) throws IOException
    {
	FileOutputStream out = new FileOutputStream( f );
	out.write( data );
	out.close();
    }

    /**
     * Given a file, read a jpeg image from that file.
     * @param width The width of the image.
     * @param height The height of the image.
     * @param frameno The frame number of the image.
     * @param f The file to read from.
     * @return The read image.
     * @throws IOException Thrown if for some reason we cannot read the image.
     */
    static JpegCompressedImage read( int width, int height, int frameno, File f ) throws IOException
    {
	// FIXME: get the width and height of the image from the file.
	InputStream is = new FileInputStream( f );

	// Get the size of the file
	long length = f.length();

	// You cannot create an array using a long type.
	// It needs to be an int type.
	// Before converting to an int type, check
	// to ensure that file is not larger than Integer.MAX_VALUE.
	if (length > Integer.MAX_VALUE) {
	    // File is too large
	    System.err.println( "File '" + f + "' is too large to fit in an array (it is " + length + ") bytes" );
	    return null;
	}

	// Create the byte array to hold the data
	byte[] bytes = new byte[(int)length];

	// Read in the bytes
	int offset = 0;
	int numRead = 0;
	while (offset < bytes.length
		&& (numRead=is.read(bytes, offset, bytes.length-offset)) >= 0) {
	    offset += numRead;
	}

	// Ensure all the bytes have been read in
	if (offset < bytes.length) {
	    throw new IOException("Could not completely read file "+f.getName());
	}

	// Close the input stream and return bytes
	is.close();
	return new JpegCompressedImage( width, height, frameno, bytes );
    }
}
