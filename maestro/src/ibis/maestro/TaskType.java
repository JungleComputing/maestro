package ibis.maestro;

import ibis.maestro.Job.JobIdentifier;

import java.io.Serializable;

/**
 * A task type.
 * 
 * @author Kees van Reeuwijk
 */
final class TaskType implements Serializable
{
    /** Contractual obligation. */
    private static final long serialVersionUID = 13451L;
    final int taskNo;
    final int remainingTasks;
    final int index;
    final JobIdentifier job;
    final boolean unpredictable;

    /** Constructs a new task type.
     * 
     * @param id The job this task belongs to.
     * @param taskNo The sequence number within the job.
     * @param remainingTasks The number of tasks after this one in the job.
     */
    TaskType( JobIdentifier id, int taskNo, int remainingTasks, boolean unpredictable, int index )
    {
        this.job = id;
        this.taskNo = taskNo;
        this.remainingTasks = remainingTasks;
        this.unpredictable = unpredictable;
        this.index = index;
    }

    /**
     * Returns a string representation of this type.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return "(" + job + ",task=" + taskNo + ")";
    }

    /**
     * Returns the hash code of this task type.
     * @return The hash code.
     */
    @Override
    public int hashCode() {
        return job.hashCode()*100 + taskNo;
    }

    /**
     * Returns true iff the given object is a task type that is equal
     * to this one.
     * @param obj The object to compare to.
     * @return True iff this and the given object are equal.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final TaskType other = (TaskType) obj;
        if (taskNo != other.taskNo)
            return false;
        if (job == null) {
            if (other.job != null)
                return false;
        } else if (!job.equals(other.job))
            return false;
        return true;
    }

    /**
     * Compares two task types based on priority. Returns
     * 1 if type a has more priority as b, etc.
     * @param a One of the task types to compare.
     * @param b The other task type to compare.
     * @return The comparison result.
     */
    static int comparePriorities( TaskType a, TaskType b )
    {
        if( a.taskNo>b.taskNo ) {
            return -1;
        }
        if( a.taskNo<b.taskNo ) {
            return 1;
        }
        return 0;
    }
}
