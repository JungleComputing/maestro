package ibis.maestro;

/**
 * Settings, such as debugging flags, for the Maestro software.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class Settings {
	// Timeout values.

	/** Message transmission timeout in ms on optional communication. */
	static final int OPTIONAL_COMMUNICATION_TIMEOUT = 500;

	/** Message transmission timeout in ms on essential communications. */
	static final int ESSENTIAL_COMMUNICATION_TIMEOUT = 20 * OPTIONAL_COMMUNICATION_TIMEOUT;

	/** Do we cache connections? */
	static final boolean CACHE_CONNECTIONS = true;

	/** The number of connections we maximally keep open. */
	static final int CONNECTION_CACHE_SIZE = 200;

	/** How many cache accesses unused before the entry is evicted. */
	static final int CONNECTION_CACHE_MAXIMAL_UNUSED_COUNT = 200;

	// Debugging flags.

	/** Trace type handling? */
	static final boolean traceTypeHandling = false;

	/** Trace the handling or result tasks? */
	static final boolean traceResultTasks = false;

	/** Trace the progress of the worker lists of masters. */
	static final boolean traceWorkerList = false;

	/** Trace the creation and destruction of Nodes. */
	static final boolean traceNodes = false;

	/** Trace the events in the master queue? */
	static final boolean traceMasterQueue = false;

	/** Trace the administration of remaining job time. */
	static final boolean traceRemainingJobTime = false;

	/** Trace the progress of masters? */
	static final boolean traceNodeProgress = false;

	/** Trace all send events. */
	static final boolean traceSends = false;

	/** Trace all missed deadlines. */
	static final boolean traceMissedDeadlines = false;

	/** Trace all queuing and dequeuing. */
	static final boolean traceQueuing = false;

	/** Print a reason for every wait() that we do. */
	static final boolean traceWaits = false;

	/** Print the reason a particular worker was selected. */
	static final boolean traceWorkerSelection = false;

	/** Trace the adventures of the map/reduce handler. */
	static final boolean traceMapReduce = false;

	/** Trace registration of the nodes with each other. */
	static final boolean traceRegistration = false;

	/** Trace all sent update messages. */
	static final boolean traceUpdateMessages = false;

	/** Trace changes to the allowances. */
	static final boolean traceAllowance = false;

	/** Trace the adventures of the gossip engine? */
	static final boolean traceGossip = false;

	/** Dump the master queue after each change? */
	static final boolean dumpMasterQueue = false;

	/** Dump the worker queue after each change? */
	static final boolean dumpWorkerQueue = false;

	/** Announce all submissions? */
	static final boolean traceSubmissions = false;

	/** Trace the adventures of the terminator thread? */
	static final boolean traceTerminator = false;

	/** Trace ant routing administration? */
	static final boolean traceAntRouting = true;

	// --- Configuration (tuning) constants. ----
	// Unfortunately we still need some magic numbers.

	/**
	 * Deadlines below this value in nanoseconds are meaningless because they
	 * are too short to be measured with reasonable accuracy.
	 */
	static final long MINIMAL_DEADLINE = 100 * Utils.MICROSECOND_IN_NANOSECONDS;

	/**
	 * Multiplier of the estimated completion time to get an allowance deadline
	 * for a task.
	 */
	static final long ALLOWANCE_DEADLINE_MARGIN = 3;

	/**
	 * Multiplier of the allowance deadline to get a reschedule deadline for a
	 * task.
	 */
	static final long RESCHEDULE_DEADLINE_MULTIPLIER = 2;

	/** The maximal time in ms before the gossiper gets more quotum. */
	static final long MAXIMUM_GOSSIPER_WAIT = 70;

	/** Time in ms when gossip goes stale for nodes in the same cluster. */
	static final long GOSSIP_EXPIRATION_IN_CLUSTER = 500;

	/** Time in ms when gossip goes stale for nodes not in the same cluster. */
	static final long GOSSIP_EXPIRATION_BETWEEN_CLUSTERS = 5 * GOSSIP_EXPIRATION_IN_CLUSTER;

	/**
	 * This many nodes that recently sent a task will be kept directly up to
	 * date with our state changes (instead of through the gossip system).
	 */
	static final int MAXIMAL_RECENT_MASTERS = 4;

	/**
	 * Apart from a work thread for each processor, this many extra work threads
	 * are run.
	 */
	static final int EXTRA_WORK_THREADS = 2;

	/** The default start quotum of the terminator. */
	static final double DEFAULT_TERMINATOR_START_QUOTUM = 0.5;

	/**
	 * The default additional termination quotum the terminator gets for each
	 * new node.
	 */
	static final double DEFAULT_TERMINATOR_NODE_QUOTUM = 0.1;

	/** The default sleep time in ms between node terminations. */
	static final long DEFAULT_TERMINATOR_SLEEP = 100;

	/** Should Maestro use ant routing? */
	static final boolean USE_ANT_ROUTING = System
	.getProperty("ibis.maestro.useAntRouting") != null;

	/** Don't take the time tasks spend the queues into account. */
	static final boolean IGNORE_QUEUE_TIME = true;

	/** If set, processors over their allowancee never get a new task,
	 * otherwise they can get one if they are significantly faster.
	 */
	static final boolean HARD_ALLOWANCES = false;
}
