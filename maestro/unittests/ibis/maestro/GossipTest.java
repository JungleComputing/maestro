package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.util.HashMap;

import junit.framework.TestCase;

import org.junit.Test;

/**
 * Tests for the gossip store.
 * 
 * @author Kees van Reeuwijk.
 */
public class GossipTest extends TestCase {

    private static class J1 implements AtomicJob {

        @Override
        public Object run(Object input) throws JobFailedException {
            return input;
        }

        @Override
        public boolean isSupported() {
            return true;
        }
    }
    
    private static class J2 implements AtomicJob {

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
    public void testGossip()
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

        Gossip gossip = new Gossip(jobs,null);
        JobType tj1 = jobs.getJobType(j1);
        gossip.setLocalComputeTime(tj1, 1.0);
        gossip.setWorkerQueueTimePerJob(tj1, 0.1, 1);
        JobType tj11 = jobs.getJobType(j11);
        gossip.setLocalComputeTime(tj11, 2.0);
        gossip.setWorkerQueueTimePerJob(tj11, 0.2, 2);
        JobType tj2 = jobs.getJobType(j2);
        gossip.setLocalComputeTime(tj2, 3.0);
        gossip.setWorkerQueueTimePerJob(tj2, 0.3, 3);
        double masterQueueIntervals[] = new double[] { 0.0, 0.0, 0.0, 0.0 };
        HashMap<IbisIdentifier, LocalNodeInfo> localNodeInfoMap = new HashMap<IbisIdentifier, LocalNodeInfo>();
        gossip.recomputeCompletionTimes(masterQueueIntervals, jobs, localNodeInfoMap);
        NodePerformanceInfo info = gossip.getLocalUpdate();
        System.out.println( "info=" + info );
    }
}
