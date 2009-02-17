package ibis.maestro;

class ValueVertex extends CalculationVertex {
    private double value;

    ValueVertex( double v )
    {
        value = v;
    }

    @Override
    synchronized double getValue() {
        return value;
    }
    
    synchronized void setValue( double v ) {
        value = v;
        notifyListeners();
    }

}
