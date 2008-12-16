package ibis.videoplayer;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

abstract class Image implements Serializable {
	private static final long serialVersionUID = 1L;
	protected final int width;
	protected final int height;
	protected final int frameno;

	Image( int width, int height, int frameno )
	{
		this.width = width;
		this.height = height;
		this.frameno = frameno;
	}

	abstract Image scaleDown( int i );

	abstract void write( File f ) throws IOException;
	abstract void print( File f ) throws IOException;

	/** Given a file name, load that file, and create an image.
	 * The type of image is determined by the file extension of the filename.
	 * At the moment only files ending with '.png' are supported, and create
	 * an uncompressed image.
	 * 
	 * @param f The file to load.
	 * @param frameno The frame number of the image.
	 * @return The image in the file.
	 * @throws IOException Thrown if the image cannot be read.
	 */
	static UncompressedImage load( File f, int frameno ) throws IOException
	{
		final String fnm = f.getName();
		if( fnm.endsWith( ".png" ) ){
			return UncompressedImage.loadPNG( f, frameno );
		}
		if( fnm.endsWith( ".ppm" ) ){
			return UncompressedImage.loadPPM( f, frameno );
		}
		System.err.println( "Don't know how to load a file '" + f + "'" );
		return null;
	}
}