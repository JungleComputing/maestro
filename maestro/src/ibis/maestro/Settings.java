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
    static final int OPTIONAL_COMMUNICATION_TIMEOUT = 30000;
    
    /** Message transmission timeout in ms on essential communications. */
    static final int ESSENTIAL_COMMUNICATION_TIMEOUT = 5*OPTIONAL_COMMUNICATION_TIMEOUT;

    /** The number of connections we maximally keep open. */
    static final int CONNECTION_CACHE_SIZE = 80;

    // Debugging flags.
    
    /** Trace type handling? */
    static final boolean traceTypeHandling = false;

    /** Trace the handling or result tasks? */
    static final boolean traceResultTasks = false;

    /** Trace the progress of the worker lists of masters. */
    static final boolean traceWorkerList = false;
    
    /** Trace the creation and destruction of Nodes. */
    static final boolean traceNodes = true;

    /** Trace the events in the master queue? */
    static final boolean traceMasterQueue = false;

    /** Trace the administration of remaining job time. */
    static final boolean traceRemainingJobTime = false;

    /** Trace the progress of masters? */
    static final boolean traceNodeProgress = true;

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

    /** Multiplier of the estimated completion time to
     * get an allowance deadline for a task.
     */
    static final long ALLOWANCE_DEADLINE_MARGIN = 3;

    /** Multiplier of the estimated completion time to
     * get a reschedule deadline for a task.
     */
    static final long RESCHEDULE_DEADLINE_MARGIN = 6;

    /** If set, routes tasks based on worker list shuffling instead of measured times. */
    static final boolean useShuffleRouting = false;
}
