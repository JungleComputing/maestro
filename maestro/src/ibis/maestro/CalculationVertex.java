/**
 * 
 */
package ibis.maestro;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Kees van Reeuwijk
 *
 */
abstract class CalculationVertex{
    private final List<CalculationUpdateListener> listeners = new LinkedList<CalculationUpdateListener>();

    protected void addListener( CalculationUpdateListener l )
    {
        listeners.add(l);
    }

    protected void notifyListeners()
    {
        for( CalculationUpdateListener l: listeners ){
            l.handleValueChange();
        }
    }
    
    abstract double getValue();
}
