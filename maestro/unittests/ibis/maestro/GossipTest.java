package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.steel.Estimator;
import ibis.steel.GaussianEstimator;

import java.io.Serializable;
import java.util.HashMap;

import junit.framework.TestCase;

import org.junit.Test;

/**
 * Tests for the gossip store.
 * 
 * @author Kees van Reeuwijk.
 */
public class GossipTest extends TestCase {
	private static final double eps = 0.1;

	private static class J1 implements AtomicJob {

		@Override
		public Serializable run(final Serializable input)
				throws JobFailedException {
			return input;
		}

		@Override
		public boolean isSupported() {
			return true;
		}
	}

	private static class J2 implements AtomicJob {

		@Override
		public Serializable run(final Serializable input)
				throws JobFailedException {
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
	@SuppressWarnings("synthetic-access")
	@Test
	public void testGossip() {
		final JobList jobs = new JobList();
		final Job j1 = new J1();
		final Job j11 = new J1();
		final Job j2 = new J2();

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
		final Job s = new SeriesJob(j1, j11, j2);
		jobs.registerJob(s);
		jobs.sanityCheck();
		assertEquals(4, jobs.getTypeCount());

		final Gossip gossip = new Gossip(jobs, null);
		final JobType tj1 = jobs.getJobType(j1);
		gossip.setLocalComputeTime(tj1, new GaussianEstimator(1.0, 0));
		gossip.setWorkerQueueTimePerJob(tj1, new GaussianEstimator(0.1, 0), 1);
		final JobType tj11 = jobs.getJobType(j11);
		gossip.setLocalComputeTime(tj11, new GaussianEstimator(2.0, 0));
		gossip.setWorkerQueueTimePerJob(tj11, new GaussianEstimator(0.2, 0), 2);
		final JobType tj2 = jobs.getJobType(j2);
		gossip.setLocalComputeTime(tj2, new GaussianEstimator(3.0, 0));
		gossip.setWorkerQueueTimePerJob(tj2, new GaussianEstimator(0.3, 0), 3);
		final Estimator v = new GaussianEstimator(0.2, 0);
		final Estimator[] masterQueueIntervals = new Estimator[] { v, v, v, v };
		final double completionTimes[] = new double[] { 0.5, 0.5, 0.5, 0.5 };
		final double transmissionTimes[] = new double[] { 0.4, 0.3, 0.2, 0.1 };
		final HashMap<IbisIdentifier, LocalNodeInfoList> localNodeInfoMap = new HashMap<IbisIdentifier, LocalNodeInfoList>();
		final int queueLengths[] = new int[] { 1, 1, 1, 1 };
		final LocalNodeInfoList localNodeInfo = new LocalNodeInfoList(false,
				buildLocalNodeInfoList(queueLengths, transmissionTimes,
						completionTimes));
		localNodeInfoMap.put(null, localNodeInfo);
		gossip.recomputeCompletionTimes(masterQueueIntervals, jobs,
				localNodeInfoMap);
		final NodePerformanceInfo info = gossip.getLocalUpdate();
		final Estimator[] l = info.completionInfo[3];
		// FIXME: better gossip test.
	}

	private LocalNodeInfo[] buildLocalNodeInfoList(final int[] queueLengths,
			final double[] transmissionTimes, final double[] completionTimes) {
		final LocalNodeInfo res[] = new LocalNodeInfo[queueLengths.length];
		for (int i = 0; i < queueLengths.length; i++) {
			res[i] = new LocalNodeInfo(queueLengths[i], new GaussianEstimator(
					transmissionTimes[i], 0), new GaussianEstimator(
					completionTimes[i], 0));
		}
		return res;
	}
}
