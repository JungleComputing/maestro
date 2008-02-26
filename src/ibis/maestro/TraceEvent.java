package ibis.maestro;

import java.io.Serializable;


/**
 * A trace event.
 * 
 * @author Kees van Reeuwijk
 *
 */
public abstract class TraceEvent implements Serializable, Comparable<TraceEvent> {

    protected final long time;

    /**
     * @param time
     */
    public TraceEvent( long time )
    {
	super();
	this.time = time;
    }

    /**
     * Returns a hash code for this value.
     * @return The hash code.
     */
    @Override
    public int hashCode() {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + (int) (time ^ (time >>> 32));
        return result;
    }

    /**
     * @param obj The other object to compare to.
     * @return True iff we consider this and another object equal.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final TraceEvent other = (TraceEvent) obj;
        if (time != other.time)
            return false;
        return true;
    }

    /**
     * Compares this trace event to another.
     * We order the events on their moment of occurrence,
     * or on their id number in the unlikely event that they
     * have the same tine.
     * @param other The other event to 
     * @return The comparison result: 1: this event is larger, -1: this event is smaller, 0: both are equal.
     */
    public int compareTo( TraceEvent other )
    {
        if( this.time<other.time ){
            return -1;
        }
        if( this.time>other.time ){
            return 1;
        }
        if( this instanceof WorkerRegistrationEvent ) {
            if( other instanceof WorkerRegistrationEvent ) {
        	return 0;
            }
            return -1;
        }
        if( other instanceof WorkerRegistrationEvent ) {
            return 1;
        }
        if( this instanceof WorkerSettingEvent ) {
            if( other instanceof WorkerSettingEvent ) {
        	return 0;
            }
            return -1;
        }
        if( other instanceof WorkerSettingEvent ) {
            return 1;
        }
        if( this instanceof TransmissionEvent && other instanceof TransmissionEvent ) {
            TransmissionEvent teThis = (TransmissionEvent) this;
            TransmissionEvent teOther = (TransmissionEvent) other;
            if( teThis.sent != teOther.sent) {
        	// Put sent events before receive events.
        	return teThis.sent?-1:1;
            }
            if( teThis.id<teOther.id ){
        	return -1;
            }
            if( teThis.id>teOther.id ){
        	return 1;
            }
        }
        return 0;
    }
}