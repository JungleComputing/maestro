package ibis.maestro;

import junit.framework.TestCase;

import org.junit.Test;

/**
 * Tests for the master queue.
 * 
 * @author Kees van Reeuwijk.
 */
public class MasterQueueTest extends TestCase {
    private static void addToQueue(JobType type, MasterQueue queue,
            Integer... ids) {
        for (Integer id : ids) {
            JobInstanceIdentifier jobInstance = new JobInstanceIdentifier(
                    id, null);
            JobInstance ti = new JobInstance(jobInstance, 0, type, type, 0);
            queue.add(ti);
        }
    }

    private static void removeFromQueue(MasterQueue queue, Integer... ids) {
        for (Integer id : ids) {
            if (queue.isEmpty()) {
                fail("Queue is empty, while I expected " + id);
            }
            JobInstance ti = queue.remove();

            if (ti.jobInstance.id != id) {
                fail("Unexpected task from master queue: " + ti.jobInstance.id
                        + " instead of " + id);
            }
        }
    }

    /** */
    @Test
    public void testAdd() {
        JobType type = new JobType(false, true, 0);
        JobType l[] = new JobType[] { type };
        MasterQueue queue = new MasterQueue(l);

        addToQueue(type, queue, 0);
        removeFromQueue(queue, 0);
        if (!queue.isEmpty()) {
            fail("Queue should be empty");
        }

        addToQueue(type, queue, 1, 0);
        removeFromQueue(queue, 0, 1);
        if (!queue.isEmpty()) {
            fail("Queue should be empty");
        }

        addToQueue(type, queue, 4, 3, 2);
        removeFromQueue(queue, 2);
        addToQueue(type, queue, 0, 1, 5, 6);
        removeFromQueue(queue, 0, 1, 3, 4, 5, 6);
        if (!queue.isEmpty()) {
            fail("Queue should be empty");
        }
    }

}
