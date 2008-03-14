package ibis.maestro;

/**
 * @author Kees van Reeuwijk
 *
 * A return value for a job that returns a double.
 */
public class DoubleResultValue implements JobResultValue {
    /** Contractual obligation. */
    private static final long serialVersionUID = 5711465613411304317L;
    private final double value;
    
    /** Constructs a new instance of a return
     * value.
     * @param value The value to return.
     */
    public DoubleResultValue( double value )
    {
        this.value = value;
    }

    /**
     * Gets the return value of this message.
     * @return The return value.
     */
    public double getValue() {
        return value;
    }


    /**
     * @return The string representation of this value.
     */
    @Override
    public String toString(){
        return "return value: " + value;
    }
}
