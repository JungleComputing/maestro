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
class SumCalculationVertex extends CalculationVertex implements CalculationUpdateListener {
    private double value;
    protected List<CalculationVertex> elements = new LinkedList<CalculationVertex>();

    SumCalculationVertex( CalculationVertex... calculationNodes)
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
        double nval = 0.0;

        for( CalculationVertex n: elements ) {
            nval += n.getValue();
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
}
