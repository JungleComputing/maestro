package image;

import org.apache.log4j.Logger;

public abstract class ImageQueue<T extends Image> {
    
    protected final Logger logger;
    
    protected final String name;
    protected final int maxImages;
    
    protected boolean done = false;
    
    public ImageQueue(String name, int maxImages) { 
        this.name = name;
        this.maxImages = maxImages;
    
        logger = Logger.getLogger("ImageQueue." + name);
   
        System.out.println("[*] Created queue " + name + " max length " + maxImages);
    }
    
    public ImageQueue(int maxImages) { 
        this("Anonymous", maxImages);
    }
    
    public String getName() { 
        return name;
    }
    
    public int getMaxLength() { 
        return maxImages;
    }
    
    public synchronized boolean getDone() {
        return done;
    }

    public synchronized void setDone() {
        done = true;
        notifyAll();
    }
    
    public synchronized boolean waitUntilDone() {
        
        while (!done) {
   
            try { 
                wait();
            } catch (InterruptedException e) {
                // ignore
            }
        }
        
        return false;
    }
    
    public abstract T get();
    public abstract void put(T b);    
    
    public abstract boolean putMayBlock();
    public abstract boolean getMayBlock();
    
    public abstract int getSize();
    
    public abstract void printStats();
    

}

