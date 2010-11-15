package ibis.maestro;

import ibis.ipl.IbisIdentifier;

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
		gossip.setLocalComputeTime(tj1, new TimeEstimate(1.0, 0));
		gossip.setWorkerQueueTimePerJob(tj1, new TimeEstimate(0.1, 0), 1);
		final JobType tj11 = jobs.getJobType(j11);
		gossip.setLocalComputeTime(tj11, new TimeEstimate(2.0, 0));
		gossip.setWorkerQueueTimePerJob(tj11, new TimeEstimate(0.2, 0), 2);
		final JobType tj2 = jobs.getJobType(j2);
		gossip.setLocalComputeTime(tj2, new TimeEstimate(3.0, 0));
		gossip.setWorkerQueueTimePerJob(tj2, new TimeEstimate(0.3, 0), 3);
		final TimeEstimate v = new TimeEstimate(0.2, 0);
		final TimeEstimate[] masterQueueIntervals = new TimeEstimate[] { v, v,
				v, v };
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
		final TimeEstimate[] l = info.completionInfo[3];
		assertEquals(9.5, l[0].mean, eps);
		assertEquals(0.0, l[0].variance, eps);
		assertEquals(7.7, l[1].mean, eps);
		assertEquals(0.0, l[1].variance, eps);
		assertEquals(4.6, l[2].mean, eps);
		assertEquals(0.0, l[2].variance, eps);
	}

	private LocalNodeInfo[] buildLocalNodeInfoList(final int[] queueLengths,
			final double[] transmissionTimes, final double[] completionTimes) {
		final LocalNodeInfo res[] = new LocalNodeInfo[queueLengths.length];
		for (int i = 0; i < queueLengths.length; i++) {
			res[i] = new LocalNodeInfo(queueLengths[i], new TimeEstimate(
					transmissionTimes[i], 0), new TimeEstimate(
					completionTimes[i], 0));
		}
		return res;
	}
}
