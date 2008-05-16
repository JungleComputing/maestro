package demo.grid;

import ibis.ipl.IbisIdentifier;

import java.io.Serializable;

public class ListingReply implements Serializable, Comparable<ListingReply> {

    /** 
     * Generated
     */
    private static final long serialVersionUID = 6385898547267794267L;

    private final IbisIdentifier id;    
    private final long start; 
    private final long end; 
    private final long stride;
    
    public ListingReply(IbisIdentifier id, long start, long end, long stride) {
        this.id = id;
        this.start = start;
        this.end = end;
        this.stride = stride;
    }

    public IbisIdentifier getID() { 
        return id;
    }
    
    public long getEnd() {
        return end;
    }

    public long getStart() {
        return start;
    }

    public long getStride() {
        return stride;
    }
    
    public boolean isValid() {
        return (start >= 0 && end >= 0 && stride >= 1);
    }
    
    public int compareTo(ListingReply other) {
        return (int)(start - other.start);
    }
    
    public String toString() { 
        return "ListingReply " + start + " ... " + end + ": " + stride;
    }
}
