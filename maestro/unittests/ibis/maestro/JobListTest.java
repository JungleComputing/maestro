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

    @Test
    public void testBenchmarkJobList() {
        JobList jobs = new JobList();
        J1 j1 = new J1();
        J1 j11 = new J1();
        J2 j2 = new J2();
        J2 j21 = new J2();
        J1 j12 = new J1();
        SeriesJob convertJob = new SeriesJob(j1, j2, j11,
                j21, j12);
        jobs.registerJob(convertJob);
        JobType convertJobType = jobs.getJobType(convertJob);
        JobType stage0Type = jobs.getStageType(convertJobType, 0);
        assertEquals( true, stage0Type.isAtomic );
        JobType todoList[] = jobs.getTodoList(convertJob);
        assertEquals( 5, todoList.length );
        for( JobType t: todoList ){
            assertEquals( true, t.isAtomic );
        }
        assertEquals( j1,  jobs.getJob(todoList[0]));
        assertEquals( j2,  jobs.getJob(todoList[1]));
        assertEquals( j11, jobs.getJob(todoList[2]));
        assertEquals( j21, jobs.getJob(todoList[3]));
        assertEquals( j12, jobs.getJob(todoList[4]));
        assertNotSame(convertJob, jobs.getJob(stage0Type) );
    }

    /**
     * 
     */
    @Test
    public void testJobList() {
        JobList jobs = new JobList();
        Job j1 = new J1();
        Job j11 = new J1();
        Job j2 = new J2();
        JobType tj1;
        JobType tj11;
        JobType tj2;

        jobs.sanityCheck();
        assertEquals(0, jobs.getTypeCount());
        jobs.registerJob(j1);
        jobs.sanityCheck();
        assertEquals(1, jobs.getTypeCount());
        jobs.registerJob(j1);
        jobs.sanityCheck();
        assertEquals(1, jobs.getTypeCount());
        jobs.registerJob(j11);
        jobs.sanityCheck();
        assertEquals(2, jobs.getTypeCount());
        jobs.registerJob(j2);
        jobs.sanityCheck();
        assertEquals(3, jobs.getTypeCount());
        tj1 = jobs.getJobType(j1);
        tj11 = jobs.getJobType(j11);
        tj2 = jobs.getJobType(j2);
        Job s = new SeriesJob(j1, j11, j2);
        jobs.registerJob(s);
        jobs.sanityCheck();
        assertEquals(4, jobs.getTypeCount());
        JobType ts = jobs.getJobType(s);
        // Note that we can get away with (pointer) equality comparison
        // because the system guarantees that for each particular type,
        // always the same <code>JobType</code> instance is returned.
        assertEquals(tj1, jobs.getStageType(ts, 0));
        assertEquals(tj11, jobs.getStageType(ts, 1));
        assertEquals(tj2, jobs.getStageType(ts, 2));
        Job s2 = new SeriesJob(j1, j1, j2, s);
        jobs.registerJob(s2);
        jobs.sanityCheck();
        assertEquals(5, jobs.getTypeCount());
        JobType ts2 = jobs.getJobType(s2);
        assertEquals(tj1, jobs.getStageType(ts2, 0));
        assertEquals(tj1, jobs.getStageType(ts2, 1));
        assertEquals(tj2, jobs.getStageType(ts2, 2));
        assertEquals(tj1, jobs.getStageType(ts2, 3));
        assertEquals(tj11, jobs.getStageType(ts2, 4));
        assertEquals(tj2, jobs.getStageType(ts2, 5));
        Job s3 = new SeriesJob(new J1(), new J1(), new J2(), s2);
        jobs.registerJob(s3);
        jobs.sanityCheck();
        assertEquals(9, jobs.getTypeCount());
        assertTodoListLength(jobs, j1, 1);
        assertTodoListLength(jobs, s, 3);
        assertTodoListLength(jobs, s2, 6);
        assertTodoListLength(jobs, s3, 9);
    }

    private void assertTodoListLength(JobList jobs, Job s, int i) {
        JobType todo[] = jobs.getTodoList(s);
        assertEquals(i, todo.length);
    }
}
