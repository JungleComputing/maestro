package util.config;

import image.Image;

import java.io.Serializable;
import java.util.Arrays;

public class QueueDescription implements Serializable {

    /** 
     * Generated
     */
    private static final long serialVersionUID = 2739994289541446597L;

    private final String name;
    
    private final Class<? extends Image> type;
    
    private final String [] get;
    private final String [] put;
    
    private final int getLength;
    private final int putLength;
  
    public QueueDescription(final String name, final Class<? extends Image> type, 
            final String[] put, final String[] get, final int putLength, 
            final int getLength) {
        
        this.name = name;
        this.type = type;
        this.put = put;
        this.get = get;
        this.getLength = getLength;
        this.putLength = putLength;
        
        System.out.println("New QD " + name + " " + getLength + " " + putLength);
    }

    public String getName() {
        return name;
    }
    
    public Class<? extends Image> getType() { 
        return type;
    }
    
    public String[] getGet() {
        return get;
    }

    public int getGetLength() {
        return getLength;
    }

    public String[] getPut() {
        return put;
    }

    public int getPutLength() {
        return putLength;
    }
    
    public int getTotalLength() {
        return Math.max(getLength, putLength);
    }
    
    public String toString() { 
        return name + " " + Arrays.toString(put) + " -> " + Arrays.toString(get);
    }
    
}
