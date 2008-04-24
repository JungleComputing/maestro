// File: Service.java

package ibis.maestro;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.AbstractList;
import java.util.ArrayList;

/**
 * 
 * @author Kees van Reeuwijk.
 *
 * Global helper functions.
 */
public class Service
{
    /** Prints the platform version that is used. */
    static String getPlatformVersion()
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
    
    /** Given a time in nanoseconds, return a neat format string for it.
     * 
     * @param t The time to format.
     * @return The formatted string.
     */
    public static String formatNanoseconds( final long t )
    {
	if( t == Long.MAX_VALUE ) {
	    return "infinite";
	}
	if( t == 0 ) {
	    return "0 s";
	}
	if( t<2000L && t>-2000L ) {
	    return String.format( "%d ns", t );
	}
	if( t<2000000L && t>-2000000L ) {
	    return String.format( "%4.2f us", t/1000.0 );
	}
	if( t<2000000000L && t>-2000000000L ) {
	    return String.format( "%4.2f ms", t/1000000.0 );
	}
	return String.format( "%4.2f s", t/1000000000.0 );
    }

    /** Wait for the given thread to terminate.
     * @param thread The tread to wait for.
     */
    public static void waitToTerminate( Thread thread )
    {
        do {
            try {
                thread.join();
            }
            catch( InterruptedException x ) {
                // We don't care
            }
        } while( thread.isAlive() );
    }

    static boolean member( AbstractList<MasterInfo> l, MasterInfo e )
    {
        for( MasterInfo mi: l ) {
            if( mi == e ) {
        	return true;
            }
        }
        return false;
    }

    static boolean member( ArrayList<JobType> jobTypes, JobType jobType )
    {
        for( JobType t: jobTypes ) {
            if( t.equals( jobType ) ) {
        	return true;
            }
        }
        return false;
    }

}
