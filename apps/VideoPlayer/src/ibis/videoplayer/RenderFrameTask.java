package ibis.videoplayer;

import ibis.maestro.AtomicTask;
import ibis.maestro.Node;
import ibis.util.RunProcess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;

/**
 * An task to run a PovRay scene description through povray, and load the
 * resulting image.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class RenderFrameTask implements AtomicTask
{
    private static final long serialVersionUID = -3938044583266505212L;
    private static final File tmpDir = new File( "/tmp" );  // FIXME: be more paranoid than this.

    /**
     * Returns the name of this task.
     * @return The name.
     */
    @Override
    public String getName()
    {
	return "Render frame";
    }

    static class RenderInfo implements Serializable
    {
	private static final long serialVersionUID = 1899219003828691971L;
	final int width;
	final int height;
	final int startRow;
	final int endRow;
	final int startColumn;
	final int endColumn;
	final int frameno;
	final String scene;

	/**
	 * @param width The width of the frame to render.
	 * @param height The height of the frame to render.
	 * @param frame The number of the frame to render.
	 * @param scene The scene file.
	 */
	RenderInfo( int width, int height, int startColumn, int endColumn, int startRow, int endRow, int frame, String scene ) {
	    this.width = width;
	    this.height = height;
	    this.startColumn = startColumn;
	    this.endColumn = endColumn;
	    this.startRow = startRow;
	    this.endRow = endRow;
	    this.frameno = frame;
	    this.scene = scene;
	}
    }

    /**
     * Given a filename, try to read that file.
     * @param f The name of the file to read.
     * @return The contents of the file, or null.
     */
    static String readFile( File f)
    {
	final int len = (int) f.length(); 
	char buf[] = new char[len];
	BufferedReader br = null;

	try {
	    int readLength = 0;  // How many bytes do we already have?
	    br = new BufferedReader( new FileReader( f ) );
	    while( readLength<len ) {
		int numRead = br.read( buf, readLength, len-readLength );
		if( numRead<0 ) {
		    // Surprisingly, we have reached the end of the file.
		    System.err.println( "Short read on file '" + f + "': only " + readLength + " instead of " + len + " bytes" );
		    break;
		}
		readLength += numRead;
	    }
	}
	catch( IOException e ) {
	    buf = null;
	}
	finally {
	    if( br != null ) {
		try{
		    br.close();
		}
		catch( IOException e ){
		    // Nothing we can do about it.
		}
	    }
	}
	if( buf == null ) {
	    return null;
	}
	return new String( buf );
    }

    RenderInfo loadScene( File f, int width, int height, int startRow, int endRow, int startColumn, int endColumn, int frameno )
    {
	String scene = readFile( f );
	return new RenderInfo( width, height, startRow, endRow, startColumn, endColumn, frameno, scene );
    }

    private static boolean writeFile( File f, String s ) throws IOException
    {
	boolean ok = f.delete();  // First make sure it doesn't exist.
	FileWriter output = new FileWriter( f );
	output.write( s );
	output.close();
	return ok;
    }

    static UncompressedImage renderImage( int width, int height, int startRow, int endRow, int startColumn, int endColumn, int frameno, String scene )
    {
	File povFile = null;
	File outFile = null;
	UncompressedImage img = null;

	String povrayExecutable = System.getenv( "POVRAY" );
	if( povrayExecutable == null ) {
	    povrayExecutable = "/usr/bin/povray";
	}
	System.out.println( "Rendering frame " + frameno );
	try {
	    povFile = File.createTempFile( String.format( "fr-%06d", frameno ), ".pov", tmpDir );
	    outFile = File.createTempFile( String.format( "fr-%06d", frameno ), ".ppm", tmpDir );
	    writeFile( povFile, scene );
	}
	catch( IOException e ) {
	    System.err.println( "Cannot write render input file: " + e.getLocalizedMessage() );
	    return null;
	}
	String command[] = {
		povrayExecutable,
		povFile.getAbsolutePath(),
		"+O" + outFile.getAbsolutePath(),
		"+SR" + startRow,
		"+ER" + endRow,
		"+SC" + startColumn,
		"+EC" + endColumn,
		"+H" + height,
		"+W" + width,
		"-D",  // No output display
		"-GA", // No output (ignored by some povray versions)
		"+FP16",
		"+Q9",
		"+A0.5"
	};
	try {
	    RunProcess p = new RunProcess( command );
	    p.run();
	    int exit = p.getExitStatus();
	    if( exit != 0 ) {
		String cmd = "";
		for( String c: command ) {
		    if( !cmd.isEmpty() ) {
			cmd += ' ';
		    }
		    cmd += c;
		}
		System.err.println( "Render command '" + cmd + "' failed:" );
		System.err.println( new String( p.getStdout() ) );
		System.err.println( new String( p.getStderr() ) );
		return null;
	    }
	    img = Image.load( outFile, frameno );
	    povFile.delete();
	    outFile.delete();
	}
	catch( IOException e ) {
	    System.err.println( "Cannot run renderer: " + e.getLocalizedMessage() );
	    return null;
	}
	return img;

    }

    /** Runs this render task.
     * @return The rendered frame.
     */
    @Override
    public Object run( Object obj, Node node )
    {
	RenderInfo info = (RenderInfo) obj;
	return renderImage( info.width, info.height, info.startRow, info.endRow, info.startColumn, info.endColumn, info.frameno, info.scene );
    }

    /**
     * @return True, because this task can run anywhere.
     */
    @Override
    public boolean isSupported()
    {
	// FIXME: test whether PovRay works.
	return true;
    }
}
