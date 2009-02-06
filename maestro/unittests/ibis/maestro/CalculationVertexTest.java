package ibis.maestro;

import junit.framework.TestCase;

import org.junit.Test;

/**
 * Tests the calculation vertices.
 * 
 * @author Kees van Reeuwijk.
 */
public class CalculationVertexTest extends TestCase {
    // Maximal allowed comparison error.
    private static final double eps = 0.1;

    /** */
    @Test
    public void testVertices() {
        ValueVertex a = new ValueVertex( 2.0 );
        ValueVertex b = new ValueVertex( 3.0 );
        ValueVertex c = new ValueVertex( 7.0 );
        ValueVertex d = new ValueVertex( 1.0 );
        SumCalculationVertex sab = new SumCalculationVertex( a, b );
        SumCalculationVertex scd = new SumCalculationVertex( c, d );
        MaxCalculationVertex max = new MaxCalculationVertex( sab, scd );
        MinCalculationVertex min = new MinCalculationVertex( sab, scd );
        assertEquals(5, sab.getValue(), eps);
        assertEquals(8, scd.getValue(), eps);
        assertEquals(8, max.getValue(), eps);
        assertEquals(5, min.getValue(), eps);
               
        a.setValue(6.0);
        assertEquals( 9, sab.getValue(), eps);
        assertEquals( 9, max.getValue(), eps);
        assertEquals( 8, min.getValue(), eps);
        
        ValueVertex e = new ValueVertex( 3 );
        sab.add( e );
        scd.add( e );

        assertEquals(12, sab.getValue(), eps);
        assertEquals(11, scd.getValue(), eps);
        assertEquals(12, max.getValue(), eps);
        assertEquals(11, min.getValue(), eps);
        
        e.withdrawFromGraph();
        assertEquals( 9, sab.getValue(), eps);
        assertEquals( 8, scd.getValue(), eps);
        assertEquals( 9, max.getValue(), eps);
        assertEquals( 8, min.getValue(), eps);
    }
}
