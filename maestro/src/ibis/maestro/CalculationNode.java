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
abstract class CalculationNode implements CalculationUpdateListener {
    private final List<CalculationUpdateListener> listeners = new LinkedList<CalculationUpdateListener>();
    
    protected void notifyListeners()
    {
        for( CalculationUpdateListener l: listeners ){
            l.handleValueChange();
        }
    }
}
