package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * Abstract superclass of messages that the master sends to the worker.
 * @author Kees van Reeuwijk
 *
 */
public class TaskMessage extends MasterMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 592770692832628690L;
    private final Job job;
    private final long id;
    private final ReceivePortIdentifier master;

    public TaskMessage( Job job, long id, ReceivePortIdentifier master )
    {
        this.job = job;
        this.id = id;
        this.master = master;
    }
    public Job getJob() {
        return job;
    }

    public long getId() {
        return id;
    }

    public ReceivePortIdentifier getMaster() {
        return master;
    }

}
