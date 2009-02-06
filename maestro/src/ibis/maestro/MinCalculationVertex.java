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
class MinCalculationVertex extends CalculationVertex implements CalculationUpdateListener {
    private double value;
    protected List<CalculationVertex> elements = new CopyOnWriteArrayList<CalculationVertex>();

    MinCalculationVertex( CalculationVertex... calculationNodes)
    {
        add(calculationNodes);
    }
    
    /**
     * One of the input nodes has changed, update the value
     * of this node.
     */
    public void handleValueChange() {
        boolean changed;

        synchronized( this ){
            double nval = Double.NaN;
            for( CalculationVertex n: elements ) {
                double v = n.getValue();

                if( Double.isNaN( nval ) || v<nval ) {
                    nval = v;
                }
            }
            changed = nval != value;
            value = nval;
        }
        if( changed ) {
            notifyListeners();
        }
    }

    @Override
    synchronized double getValue() {
        return value;
    }

    void add(CalculationVertex... nl) {
        for( CalculationVertex e: nl ) {
            elements.add(e);
            e.addListener( this );
        }
        handleValueChange();
    }

    @Override
    public void withdrawVertex(CalculationVertex v) {
        elements.remove( v );
        v.removeListener( this );
        handleValueChange();
    }
}
