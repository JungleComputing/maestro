package ibis.maestro;

import java.io.Serializable;
import java.util.Arrays;

import junit.framework.TestCase;

import org.junit.Test;

/**
 * Tests for the master queue.
 * 
 * @author Kees van Reeuwijk.
 */
public class MasterQueueTest extends TestCase {
    private static void addToQueue(JobList jobs, JobType type,
            MasterQueue queue, Integer... ids) {
        for (Integer id : ids) {
            JobInstanceIdentifier jii = new JobInstanceIdentifier(id, id, null);
            JobInstance ti = new JobInstance(jii, 0, type, 0);
            queue.add(jobs, ti);
        }
    }

    private static void removeFromQueue(MasterQueue queue, Integer... ids) {
        for (Integer id : ids) {
        	long idl[] = new long[]{ id };
            if (queue.isEmpty()) {
                fail("Queue is empty, while I expected " + id);
            }
            JobInstance ti = queue.remove();

            if (!Arrays.equals(ti.jobInstance.ids,idl)) {
                fail("Unexpected task from master queue: " + ti.jobInstance.ids
                        + " instead of " + id);
            }
        }
    }

    private static class J1 implements AtomicJob {
        @Override
        public boolean isSupported() {
            return true;
        }

        @Override
        public Serializable run(Serializable input) throws JobFailedException {
            return input;
        }
    }

    /** */
    @SuppressWarnings("synthetic-access")
    @Test
    public void testAdd() {
        JobList jobs = new JobList();
        JobType type = jobs.registerJob(new J1());
        JobType l[] = jobs.getAllTypes();
        MasterQueue queue = new MasterQueue(l);

        addToQueue(jobs, type, queue, 0);
        removeFromQueue(queue, 0);
        if (!queue.isEmpty()) {
            fail("Queue should be empty");
        }

        addToQueue(jobs, type, queue, 1, 0);
        removeFromQueue(queue, 0, 1);
        if (!queue.isEmpty()) {
            fail("Queue should be empty");
        }

        addToQueue(jobs, type, queue, 4, 3, 2);
        removeFromQueue(queue, 2);
        addToQueue(jobs, type, queue, 0, 1, 5, 6);
        removeFromQueue(queue, 0, 1, 3, 4, 5, 6);
        if (!queue.isEmpty()) {
            fail("Queue should be empty");
        }
    }

}
