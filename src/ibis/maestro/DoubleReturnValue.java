package ibis.maestro;

public class DoubleReturnValue implements JobReturn {
    private final double value;
    
    public DoubleReturnValue( double value )
    {
        this.value = value;
    }

    public double getValue() {
        return value;
    }

}
