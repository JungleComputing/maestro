package ibis.maestro;

import ibis.maestro.LabelTracker.Label;
import junit.framework.TestCase;

import org.junit.Test;

/**
 * Tests for the gossip store.
 * 
 * @author Kees van Reeuwijk.
 */
public class LabelTrackerTest extends TestCase {
    private static void assertTrackerState( LabelTracker l, int issued, int returned, boolean allReturned )
    {
        assertEquals( issued, l.getIssuedLabels() );
        assertEquals( returned, l.getReturnedLabels() );
        assertEquals( allReturned, l.allAreReturned() );
        Label[] ol = l.listOutstandingLabels();
        assertEquals( issued-returned, ol.length );
    }

    /**
     * 
     */
    @Test
    public void testLabelTracker()
    {
        LabelTracker l = new LabelTracker();

        assertTrackerState( l, 0, 0, true );
        Label l1 = l.nextLabel();
        assertTrackerState( l, 1, 0, false );
        l.returnLabel(l1);
        assertTrackerState( l, 1, 1, true );
        l.returnLabel(l1);
        assertTrackerState( l, 1, 1, true );
        l1 = l.nextLabel();
        assertTrackerState( l, 2, 1, false );
        Label l2 = l.nextLabel();
        assertTrackerState( l, 3, 1, false );
        Label l3 = l.nextLabel();
        assertTrackerState( l, 4, 1, false );
        l.returnLabel(l3);
        assertTrackerState( l, 4, 2, false );
        l.returnLabel(l1);
        assertTrackerState( l, 4, 3, false );
        l.returnLabel(l2);
        assertTrackerState( l, 4, 4, true );
        l.returnLabel(l2);
        assertTrackerState( l, 4, 4, true );
    }
}
