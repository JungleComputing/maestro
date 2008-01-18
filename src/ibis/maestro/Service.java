// File: Service.java

package ibis.maestro;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;

/**
 * 
 * @author Kees van Reeuwijk.
 *
 * Global helper functions.
 */
public class Service
{

    /**
     * Given the path of a file or directory, delete that file or directory, and
     * any contained files.
     * 
     * @param path The path of the file or directory to delete.
     */
    public static void delete_directory_tree( File path )
    {
        File files[] = path.listFiles();

        if( files != null ){
        	for( File f: files ){
                delete_directory_tree( f );
            }
        }
        path.delete();
    }

    /**
     * Given a path, make sure that there is a directory there. If Necessary
     * create it.
     * 
     * @param path The path of the directory to ensure is there.
     */
    public static void ensure_dir_presence( File path )
    {
        path.mkdirs();
    }

    /**
     * Given a file <code>f</code> and a string <code>s</code>, create the given file, and fill it
     * with the text in <code>s</code>.
     * @param f The file to create.
     * @param s The contents of the file.
     * @throws IOException
     */
    public static void writeFile( File f, String s ) throws IOException
    {
    	f.delete();  // First make sure it doesn't exist.
        FileWriter output = new FileWriter( f );
        output.write( s );
        output.close();
    }

    /**
     * Given a filename, try to read that file.
     * @param f The name of the file to read.
     * @return The contents of the file, or null.
     */
    public static String readFile( File f)
    {
        final int len = (int) f.length(); 
        char buf[] = new char[len];

        try {
            BufferedReader br = new BufferedReader( new FileReader( f ) );
            br.read( buf, 0, len );
            br.close();
        }
        catch( IOException e ) {
            return null;
        }
        return new String( buf );
    }

    /** Prints the platform version that is used. */
    private static String getPlatformVersion()
    {
        java.util.Properties p = System.getProperties();
        
        return "Java " + p.getProperty( "java.version" ) + " (" + p.getProperty( "java.vendor" ) + ") on " + p.getProperty( "os.name" ) + ' ' + p.getProperty( "os.version" ) + " (" + p.getProperty( "os.arch" ) + ')';
    }

    /** Prints the given string to the tracing output. 
     * @param s The string to print to the tracing output.
     */
    public static void trace(String s ) {
        System.out.println( s );
    }

    /** Given an input stream, reads the entire contents of that stream into a String.
     * @param s The input stream to read.
     * @return A string containing the entire stream.
     * @throws IOException Thrown when there is an I/O problem.
     */
    public static String read( InputStream s ) throws IOException
    {
        InputStreamReader r = new InputStreamReader( s );

        StringBuffer res = new StringBuffer();
        int sz = 1000;
        char buffer[] = new char[sz];

        for(;;){
            int n = r.read( buffer, 0, sz );
            if( n<0 ) {
                break;
            }
            res.append( buffer, 0, n );
        }
        return new String( res );
    }
    
    private static final DecimalFormat nsFormatter = new DecimalFormat( "00#s ###ms ###us ###ns" );

    /** Given a time in nanoseconds, return a neat format string for it.
     * 
     * @param t The time to format.
     * @return The formatted string.
     */
    public static String formatNanoseconds( long t )
    {
        return nsFormatter.format( t );
    }

}
