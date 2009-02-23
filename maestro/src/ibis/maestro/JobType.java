package ibis.maestro;

import java.io.Serializable;

/**
 * A job type.
 * 
 * @author Kees van Reeuwijk
 */
final class JobType implements Serializable {
    /** Contractual obligation. */
    private static final long serialVersionUID = 13451L;

    final int index;

    final boolean unpredictable;

    /**
     * Constructs a new job type.
     * 
     */
    JobType(boolean unpredictable, int index) {
        this.unpredictable = unpredictable;
        this.index = index;
    }

    /**
     * Returns a string representation of this type.
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        return "(J" + index + ")";
    }

    /**
     * Returns the hash code of this job type.
     * 
     * @return The hash code.
     */
    @Override
    public int hashCode() {
        return index;
    }

    /**
     * Returns true iff the given object is a job type that is equal to this
     * one.
     * 
     * @param obj
     *            The object to compare to.
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
        final JobType other = (JobType) obj;
        return this.index == other.index;
    }
}
