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
class SumCalculationVertex extends CalculationVertex implements
        CalculationUpdateListener {
    private double value;

    private final List<CalculationVertex> elements = new CopyOnWriteArrayList<CalculationVertex>();

    SumCalculationVertex(CalculationVertex... calculationNodes) {
        add( calculationNodes );
    }

    /**
     * One of the input nodes has changed, update the value of this node.
     */
    public void handleValueChange() {
        boolean changed;
        
        synchronized( this ){
            double nval = 0.0;

            for (CalculationVertex n : elements) {
                nval += n.getValue();
            }
            changed = (nval != value);
            value = nval;
        }
        if( changed ){
            notifyListeners();
        }
    }

    @Override
    double getValue() {
        return value;
    }

    void add(CalculationVertex... nl) {
        for (CalculationVertex e : nl) {
            elements.add(e);
            e.addListener( this );
        }
        handleValueChange();
    }

    @Override
    public void withdrawVertex(CalculationVertex v) {
        elements.remove(v);
        v.removeListener( this );
        handleValueChange();
    }
}
