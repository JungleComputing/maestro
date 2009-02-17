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

    protected final void addListener( CalculationUpdateListener l )
    {
        listeners.add(l);
    }
    
    protected final void removeListener( CalculationUpdateListener l )
    {
        listeners.remove( l );
    }

    protected final void notifyListeners()
    {
        for( CalculationUpdateListener l: listeners ){
            l.handleValueChange();
        }
    }
    
    protected final void withdrawFromGraph()
    {
        for( CalculationUpdateListener l: listeners ) {
            l.withdrawVertex( this );
        }
    }

    abstract double getValue();
}
