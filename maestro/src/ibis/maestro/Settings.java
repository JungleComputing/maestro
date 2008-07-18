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

    static final double RESEARCH_BUDGET_FOR_NEW_WORKER = 0.8;
    static final double RESEARCH_BUDGET_PER_TASK = 0.08;


    // Debugging flags.
    
    /** Trace type handling? */
    static final boolean traceTypeHandling = false;

    /** Trace the progress of workers? */
    static final boolean traceWorkerProgress = false;

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
    static final boolean traceMasterProgress = false;

    /** Trace all send events. */
    static final boolean traceSends = false;

    /** Trace all queuing and dequeuing. */
    static final boolean traceQueuing = false;

    /** Print a reason for every wait() that we do. */
    static final boolean traceWaits = false;

    /** Print the reason a particular worker was selected. */
    static final boolean traceWorkerSelection = true;

    /** Multiplier of the estimated completion time to
     * get a deadline for a task.
     */
    static final long DEADLINE_MARGIN = 3;

    /** If set, routes tasks based on worker list shuffling instead of measured times. */
    static final boolean useShuffleRouting = false;
}
