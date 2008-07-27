package ibis.maestro;

/**
 * A boolean with synchronous access.
 *
 * @author Kees van Reeuwijk.
 */
public class Flag
{
    private boolean flag;
    
    Flag( boolean flag )
    {
        this.flag = flag;
    }
    
    synchronized void set()
    {
        flag = true;
    }
    
    synchronized void reset()
    {
        flag = false;
    }
    
    synchronized void set( boolean val )
    {
        flag = val;
    }
    
    synchronized boolean isSet()
    {
        return flag;
    }
}
