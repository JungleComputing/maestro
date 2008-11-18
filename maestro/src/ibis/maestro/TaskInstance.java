package ibis.maestro;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * The representation of a task instance.
 * @author Kees van Reeuwijk
 *
 */
class TaskInstance implements Serializable {
    private static final long serialVersionUID = -5669565112253289488L;
    final JobInstanceIdentifier jobInstance;
    final TaskType type;
    final Object input;
    private boolean orphan = false;
    final ArrayList<AntPoint> antTrail;

    /**
     * @param tii The job this task belongs to.
     * @param type The type of this task instance.
     * @param input The input for this task.
     */
    TaskInstance( JobInstanceIdentifier tii, TaskType type, Object input, ArrayList<AntPoint> antTrail )
    {
	jobInstance = tii;
	this.type = type;
	this.input = input;
	this.antTrail = antTrail;
    }

    String formatJobAndType()
    {
	return "(jobId=" + jobInstance.id + ",type=" + type + ")";
    }
    /**
     * Returns a string representation of this task instance.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "(task instance: job instance=" + jobInstance + " type=" + type + " input=" + input + ")";
    }

    String shortLabel()
    {
        return jobInstance.label() + "#" + type;
    }

    void setOrphan()
    {
	orphan = true;
    }
    
    boolean isOrphan()
    {
	return orphan;
    }

    /**
     * Returns the hash code of this task instance.
     * @return The hash code.
     */
    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result
		+ ((jobInstance == null) ? 0 : jobInstance.hashCode());
	result = prime * result + ((type == null) ? 0 : type.hashCode());
	return result;
    }

    /**
     * @param obj
     * @return
     */
    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	final TaskInstance other = (TaskInstance) obj;
	if (jobInstance == null) {
	    if (other.jobInstance != null)
		return false;
	} else if (!jobInstance.equals(other.jobInstance))
	    return false;
	if (type == null) {
	    if (other.type != null)
		return false;
	} else if (!type.equals(other.type))
	    return false;
	return true;
    }

}
