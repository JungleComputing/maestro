/**
 * 
 */
package ibis.maestro;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Kees van Reeuwijk
 *
 */
abstract class CalculationVertex {
    private final List<CalculationUpdateListener> listeners = new CopyOnWriteArrayList<CalculationUpdateListener>();

    protected void addListener( CalculationUpdateListener l )
    {
        listeners.add(l);
    }
    
    protected void removeListener( CalculationUpdateListener l )
    {
        listeners.remove( l );
    }

    protected void notifyListeners()
    {
        for( CalculationUpdateListener l: listeners ){
            l.handleValueChange();
        }
    }
    
    protected void withdrawFromGraph()
    {
        for( CalculationUpdateListener l: listeners ) {
            l.withdrawVertex( this );
        }
    }

    abstract double getValue();
}
