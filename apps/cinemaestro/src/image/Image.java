package image;

import java.io.Serializable;

public abstract class Image implements Serializable {
    
    public final long number;
    
    public final Object metaData;    
     
    public Image(long number, Object metaData) { 
        this.number = number;
        this.metaData = metaData; 
    }
    
    public abstract Object getData();  
    public abstract long getSize();  
    
}
