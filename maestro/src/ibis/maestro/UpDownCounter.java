package ibis.maestro;

/**
 * A simple synchronized up/down counter.
 *
 * @author Kees van Reeuwijk.
 */
public class UpDownCounter
{
    private int value = 0;

    synchronized void up()
    {
        value++;
    }
    
    synchronized void down()
    {
        value--;
    }
    
    synchronized int get()
    {
        return value;
    }

    synchronized boolean isAbove( int v )
    {
        return this.value>v;
    }
    
    synchronized boolean isBelow( int v )
    {
        return this.value<v;
    }
    
    /**
     * Returns a string representation of this counter. (Overrides method in superclass.)
     * @return The string representation.
     */
    @Override
    synchronized public String toString()
    {
        return Integer.toString( value );
    }

    synchronized boolean isLessOrEqual( int v )
    {
        return this.value<=v;
    }
}
