package ibis.maestro;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * A testsuite for the Maestro code.
 *
 * @author Kees van Reeuwijk.
 */
public class AllTests
{

    /**
     * Constructs a testsuite.
     * @return The testsuite.
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite( "Test for ibis.maestro" );
        //$JUnit-BEGIN$
        suite.addTestSuite( WorkerQueueTest.class );
        suite.addTestSuite( MasterQueueTest.class );
        //$JUnit-END$
        return suite;
    }

}
