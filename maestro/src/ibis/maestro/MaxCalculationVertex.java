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
class MaxCalculationVertex extends CalculationVertex implements CalculationUpdateListener {
    private double value;
    protected List<CalculationVertex> elements = new CopyOnWriteArrayList<CalculationVertex>();

    MaxCalculationVertex( CalculationVertex... calculationNodes)
    {
        for( CalculationVertex e: calculationNodes) {
            elements.add(e);
            e.addListener( this );
        }
        handleValueChange();
    }
    
    /**
     * One of the input nodes has changed, update the value
     * of this node.
     */
    public void handleValueChange() {
        double nval = Double.NaN;

        for( CalculationVertex n: elements ) {
            double v = n.getValue();

            if( Double.isNaN( nval ) || v>nval ) {
                nval = v;
            }
        }
        if( nval != value ) {
            value = nval;
            notifyListeners();
        }
    }

    @Override
    double getValue() {
        return value;
    }

    void add(CalculationVertex... nl) {
        for( CalculationVertex e: nl ) {
            elements.add(e);
        }
    }

    @Override
    public void withdrawVertex(CalculationVertex v) {
        elements.remove( v );
    }
}
