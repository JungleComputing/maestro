package ibis.videoplayer;

/**
 * Some helper methods.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class Service {
    /** Prints the platform version that is used. */
    static String getPlatformVersion()
    {
        java.util.Properties p = System.getProperties();
        
        return "Java " + p.getProperty( "java.version" ) + " (" + p.getProperty( "java.vendor" ) + ") on " + p.getProperty( "os.name" ) + ' ' + p.getProperty( "os.version" ) + " (" + p.getProperty( "os.arch" ) + ')';
    }
}
