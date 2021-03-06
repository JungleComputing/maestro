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

    /** Trace the progress of the worker lists of masters. */
    static final boolean traceWorkerList = false;

    /** Trace the creation and destruction of Nodes. */
    static final boolean traceNodes = true;

    /** Trace the events in the master queue? */
    static final boolean traceMasterQueue = false;

    /** Trace the administration of remaining job time. */
    static final boolean traceRemainingJobTime = false;

    /** Trace the progress of masters? */
    static final boolean traceNodeProgress = false;

    /** Trace all send events. */
    static final boolean traceSends = false;

    /** Trace all missed deadlines. */
    static final boolean traceMissedDeadlines = true;

    /** Trace all queuing and dequeuing. */
    static final boolean traceQueuing = false;

    /** Print a reason for every wait() that we do. */
    static final boolean traceWaits = false;

    /** Print the reason a particular worker was selected. */
    static final boolean traceWorkerSelection = false;

    /** Trace the adventures of the map/reduce handler. */
    static final boolean traceParallelJobs = false;

    /** Trace registration of the nodes with each other. */
    static final boolean traceRegistration = false;

    /** Trace all sent update messages. */
    static final boolean traceUpdateMessages = false;

    /** Trace the adventures of the gossip engine? */
    static final boolean traceGossip = false;

    /** Trace stochastic computations (additions, multiplications). */
    static final boolean traceStochasticComputations = false;

    /** Dump the master queue after each change? */
    static final boolean dumpMasterQueue = false;

    /** Dump the worker queue after each change? */
    static final boolean dumpWorkerQueue = false;

    /** Announce all submissions? */
    static final boolean traceSubmissions = false;

    /** Trace the adventures of the terminator thread? */
    static final boolean traceTerminator = true;

    // --- Configuration (tuning) constants. ----
    // Unfortunately we still need some magic numbers.

    /**
     * Deadlines below this value in seconds are meaningless because they are
     * too short to be measured with reasonable accuracy.
     */
    static final double MINIMAL_DEADLINE = 100 * Utils.MICROSECOND;

    /**
     * Multiplier of the estimated completion time to get an allowance deadline
     * for a job.
     */
    static final long ALLOWANCE_DEADLINE_MARGIN = 3;

    /**
     * Multiplier of the allowance deadline to get a reschedule deadline for a
     * job.
     */
    static final long RESCHEDULE_DEADLINE_MULTIPLIER = 2;

    /** Time in ms when gossip goes stale for nodes in the same cluster. */
    static final long GOSSIP_EXPIRATION_IN_CLUSTER = 500;

    /** Time in ms when gossip goes stale for nodes not in the same cluster. */
    static final long GOSSIP_EXPIRATION_BETWEEN_CLUSTERS = 5 * GOSSIP_EXPIRATION_IN_CLUSTER;

    /**
     * This many nodes that recently sent a job will be kept directly up to date
     * with our state changes (instead of through the gossip system).
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

    /** Don't take the time jobs spend the queues into account. */
    static final boolean IGNORE_QUEUE_TIME = false;

    /**
     * If set, processors over their allowance never get a new job, otherwise
     * they can get one if they are significantly faster.
     */
    static final boolean HARD_ALLOWANCES = true;

    /** Time in ms between polling attempts for free room in the master queue. */
    static final long ROOM_POLL_INTERVAL = 10L;

    /** The number of jobs in the master queue before we start blocking. */
    static final int MAESTRO_MASTER_ROOM = 25;

    /**
     * The minimal duration in ns of a running job before we consider submitting
     * it again.
     */
    static final double LATE_JOB_DURATION = 300 * Utils.MILLISECOND;

    static final int MAXIMAL_QUEUE_FOR_PREDICTABLE = 1;
}
