package ibis.videoplayer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;

import ibis.maestro.Context;
import ibis.maestro.Node;
import ibis.util.RunProcess;

/**
 * An action to run a PovRay scene description through povray, and load the
 * resulting image.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class RenderFrameJob implements ibis.maestro.Job
{
    private static final long serialVersionUID = -3938044583266505212L;
    // FIXME: be more paranoid than  this.
    private static final File tmpDir = new File( "/tmp" );
    private final String iniFileContent;

    /**
     * @param iniFileContent
     */
    RenderFrameJob( String iniFileContent )
    {
        this.iniFileContent = iniFileContent;
    }

    class RenderInfo implements Serializable
    {
        private static final long serialVersionUID = 1899219003828691971L;
        final int width;
        final int height;
        final int frameno;
        final String scene;

        /**
         * @param width
         * @param height
         * @param frame
         * @param scene
         */
        RenderInfo(int width, int height, int frame, String scene) {
            this.width = width;
            this.height = height;
            this.frameno = frame;
            this.scene = scene;
        }
    }

    private static boolean writeFile( File f, String s ) throws IOException
    {
        boolean ok = f.delete();  // First make sure it doesn't exist.
        FileWriter output = new FileWriter( f );
        output.write( s );
        output.close();
        return ok;
    }

    /** Runs this render job.
     * @return The rendered frame.
     */
    @Override
    public Object run( Object obj, Node node, Context context )
    {
        RenderInfo info = (RenderInfo) obj;
        File povFile = null;
        File iniFile = null;
        File outFile = null;
        UncompressedImage img = null;

        try {
            povFile = File.createTempFile( String.format( "frame-%06d", info.frameno ), ".pov", tmpDir );
            iniFile = File.createTempFile( String.format( "frame-%06d", info.frameno ), ".ini", tmpDir );
            outFile = File.createTempFile( String.format( "frame-%06d", info.frameno ), ".ppm", tmpDir );
            writeFile( povFile, info.scene );
            writeFile( iniFile, iniFileContent );
        }
        catch( IOException e ) {
            System.err.println( "Cannot write render input file: " + e.getLocalizedMessage() );
            return null;
        }
        String command[] = {
                "povray",
                iniFile.getAbsolutePath(),
                "+I" + povFile.getAbsolutePath(),
                "+O" + outFile.getAbsolutePath(),
                "+H" + info.height,
                "+W" + info.width,
                "+FP",
                "+Q9",
                "+A0.3"
        };
        try {
            RunProcess p = new RunProcess( command );
            int exit = p.getExitStatus();
            if( exit != 0 ) {
                System.err.println( "Rendering of '" + povFile + "' failed:" );
                System.err.println( p.getStdout() );
                System.err.println( p.getStderr() );
                return null;
            }
            img = UncompressedImage.load( outFile, info.frameno );
            povFile.delete();
            iniFile.delete();
            outFile.delete();
        }
        catch( IOException e ) {
            System.err.println( "Cannot run renderer: " + e.getLocalizedMessage() );
            return null;
        }
        return img;
    }
}
