package ibis.maestro;

import junit.framework.TestCase;

import org.junit.Test;

/**
 * Tests for the master queue.
 * 
 * @author Kees van Reeuwijk.
 */
public class JobListTest extends TestCase {

    static class J1 implements AtomicJob {

        @Override
        public Object run(Object input) throws JobFailedException {
            return input;
        }

        @Override
        public boolean isSupported() {
            return true;
        }
    }
    
    static class J2 implements AtomicJob {

        @Override
        public Object run(Object input) throws JobFailedException {
            return input;
        }

        @Override
        public boolean isSupported() {
            return true;
        }
    }

    /**
     * 
     */
    @Test
    public void testJobList()
    {
        JobList jobs = new JobList();
        Job j1 = new J1();
        Job j11 = new J1();
        Job j2 = new J2();
 
        jobs.sanityCheck( );
        assertEquals( 0, jobs.getTypeCount());
        jobs.registerJob(j1);
        jobs.sanityCheck( );
        assertEquals( 1, jobs.getTypeCount());
        jobs.registerJob(j1);
        jobs.sanityCheck( );
        assertEquals( 1, jobs.getTypeCount());
        jobs.registerJob(j11);
        jobs.sanityCheck( );
        assertEquals( 2, jobs.getTypeCount());
        jobs.registerJob(j2);
        jobs.sanityCheck( );
        assertEquals( 3, jobs.getTypeCount());
        Job s = new SeriesJob( j1, j11, j2 );
        jobs.registerJob( s );
        jobs.sanityCheck( );
        assertEquals( 4, jobs.getTypeCount());
        Job s2 = new SeriesJob( j1, j1, j2, s );
        jobs.registerJob(s2);
        jobs.sanityCheck( );
        assertEquals( 5, jobs.getTypeCount());
        Job s3 = new SeriesJob( new J1(), new J1(), new J2(), s2 );
        jobs.registerJob(s3);
        jobs.sanityCheck( );
        assertEquals( 9, jobs.getTypeCount());
        assertTodoListLength( jobs, j1, 1 );
        assertTodoListLength( jobs, s, 3 );
        assertTodoListLength( jobs, s2, 6 );
        assertTodoListLength( jobs, s3, 9 );
    }

    private void assertTodoListLength(JobList jobs, Job s, int i) {
        JobType todo[] = jobs.getTodoList(s);
        assertEquals(i, todo.length);
    }
}
