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
    static final int OPTIONAL_COMMUNICATION_TIMEOUT = 15000;
    
    /** Message transmission timeout in ms on essential communications. */
    static final int ESSENTIAL_COMMUNICATION_TIMEOUT = 20*OPTIONAL_COMMUNICATION_TIMEOUT;

    /**
     * How many times do we try to send a registration message to an ibis? 
     */
    static final int MAXIMAL_REGISTRATION_TRIES = 20;

    /** The number of connections we maximally keep open. */
    static final int CONNECTION_CACHE_SIZE = 350;

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

    /** Trace registration of the nodes with eachother. */
    static final boolean traceRegistration = false;

    /** Trace all sent update messages. */
    static final boolean traceUpdateMessages = false;

    /** Trace the adventures of the non-essential sender. */
    static final boolean traceNonEssentialSender = false;

    /** Trace the adventures of the gossip engine? */
    static final boolean traceGossip = false;

    // --- Configuration (tuning) constants. ----
    // Unfortunately we still need some magic numbers.
    
    /** Multiplier of the estimated completion time to
     * get an allowance deadline for a task.
     */
    static final long ALLOWANCE_DEADLINE_MARGIN = 3;

    /** Multiplier of the estimated completion time to
     * get a reschedule deadline for a task.
     */
    static final long RESCHEDULE_DEADLINE_MARGIN = 6;

    /** The maximal time in ms before the gossiper gets more quotum.  */
    static final long MAXIMUM_GOSSIPER_WAIT = 10;

    /** Time in ms when gossip goes stale for nodes in the same cluster. */
    static final long GOSSIP_EXPIRATION_IN_CLUSTER = 30;

    /** Time in ms when gossip goes stale for nodes not in the same cluster. */
    static final long GOSSIP_EXPIRATION_BETWEEN_CLUSTERS = 100;

    /** This many nodes that recently sent a task will be kept directly up to
     * date with our state changes (instead of through the gossip system).
     */
    static final int MAXIMAL_RECENT_MASTERS = 4;
}
