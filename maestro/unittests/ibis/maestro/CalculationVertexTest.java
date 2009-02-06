package ibis.maestro;

import junit.framework.TestCase;

import org.junit.Test;

/**
 * Tests for the master queue.
 * 
 * @author Kees van Reeuwijk.
 */
public class CalculationVertexTest extends TestCase {
    // Maximal allowed comparison error.
    private static final double eps = 0.1;

    /** */
    @Test
    public void testAdd() {
        ValueVertex a = new ValueVertex( 2.0 );
        ValueVertex b = new ValueVertex( 3.0 );
        ValueVertex c = new ValueVertex( 7.0 );
        ValueVertex d = new ValueVertex( 1.0 );
        SumCalculationVertex sab = new SumCalculationVertex( a, b );
        SumCalculationVertex scd = new SumCalculationVertex( c, d );
        MaxCalculationVertex res = new MaxCalculationVertex( sab, scd );
        assertEquals(5, sab.getValue(), eps);
        assertEquals(8, scd.getValue(), eps);
        assertEquals(8, res.getValue(), eps);
        
        a.setValue(6.0);
        assertEquals(9, sab.getValue(), eps);
        assertEquals(9, res.getValue(), eps);
    }
}
