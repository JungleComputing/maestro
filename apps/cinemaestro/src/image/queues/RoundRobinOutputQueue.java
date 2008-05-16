package image.queues;

import image.Image;
import image.ImageQueue;

public class RoundRobinOutputQueue<T extends Image> extends ImageQueue<T> {
    
    private final GetOnlyQueue<T> [] queues; 
    
    private int index = 0;
    
    @SuppressWarnings("unchecked")
    public RoundRobinOutputQueue(String name, int maxImages, int outputs) { 
        super(name, maxImages);
   
        queues = new GetOnlyQueue[outputs];
        
        for (int i=0;i<outputs;i++) { 
            queues[i] = new GetOnlyQueue<T>(name + "." + i, maxImages);
        }
    }
    
    public RoundRobinOutputQueue(int maxImages, int outputs) { 
        this("Anonymous", maxImages, outputs);
    }
    
    public T get() {
        throw new RuntimeException("Get not allowed!");
    }
    
    public ImageQueue<T> getQueue(int index) { 
        return queues[index];
    }

    public void put(T b) {
        
        GetOnlyQueue<T> tmp = queues[index];
        
        logger.info("Put to " + index + " (length " + tmp.getSize() + " / " + maxImages + ")");
           
        tmp.secretPut(b);
        
        index = (index + 1) % queues.length;
    }
    
    public void setDone() {
        
        for (GetOnlyQueue<T> q : queues) {
            q.setDone();
        }
        
        super.setDone();
    }
   
    @Override
    public boolean getMayBlock() {
        return false;
    }

    @Override
    public boolean putMayBlock() {
        return queues[index].putMayBlock();
    }
    
    @Override
    public int getSize() {
    
        int size = 0;
        
        for (GetOnlyQueue<T> p : queues) { 
            size += p.getSize();
        }
        
        return size;
    }
    
    public static <U extends Image> RoundRobinOutputQueue<U> create(
            Class<U> type, String name, int max, int outputs) {
        
        // Not sure if this works....
        return new RoundRobinOutputQueue<U>(name, max, outputs);
    }

    @Override
    public void printStats() {
        for (GetOnlyQueue q : queues) { 
            q.printStats();
        }
  
    }
}

