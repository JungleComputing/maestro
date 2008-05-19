package image.queues;

import image.Image;
import image.ImageQueue;

public class RoundRobinInputQueue<T extends Image> extends ImageQueue<T> {
    
    private final PutOnlyQueue<T> [] queues; 
    
    private int index = 0;
    
    @SuppressWarnings("unchecked")
    public RoundRobinInputQueue(String name, int maxImages, int inputs) { 
        super(name, maxImages);
   
        queues = new PutOnlyQueue[inputs];
        
        for (int i=0;i<inputs;i++) { 
            queues[i] = new PutOnlyQueue<T>(name + "." + i, maxImages);
        }
    }
    
    public RoundRobinInputQueue(int maxImages, int outputs) { 
        this("Anonymous", maxImages, outputs);
    }
    
    private void ensureDone() { 
       
        long start = System.currentTimeMillis();        
        
        System.out.println("[*] Checking if all are done!");
        
        for (PutOnlyQueue<T> p : queues) { 
            
            while (!p.getDone()) { 
                System.out.println("[*] Waiting for queue " + p.getName());    
                p.waitUntilDone();
            }
            
            if (p.getSize() != 0) { 
                System.out.println("[*] EEP: queue " + p.getName() + " still contains data!");    
            }
            
        }
        
        long end = System.currentTimeMillis();
        
        System.out.println("[*] Ensure done took " + (end-start));
        
        // Call the real setDone of the super class
        super.setDone();
    }
    
    @Override
    public T get() {
        PutOnlyQueue<T> tmp = queues[index];
        
        logger.info("Get from " + index + " (length " + tmp.getSize() + " / " + maxImages + ")");

        T image = tmp.secretGet();
        
        if (image == null) {
            // The queue has finished!
            ensureDone();
            return null;
        }
        
        index = (index + 1) % queues.length;

        return image;
    }
    
    public ImageQueue<T> getQueue(int ix) { 
        return queues[ix];
    }

    @Override
    public void put(T b) {
        throw new RuntimeException("Get not allowed!");
    }   
   
    @Override
    public boolean getMayBlock() {
        return queues[index].getMayBlock();
    }
    
    @Override
    public boolean putMayBlock() {
        return false;
    }
    
    @Override
    public int getSize() {
    
        int size = 0;
        
        for (PutOnlyQueue<T> p : queues) { 
            size += p.getSize();
        }
        
        return size;
    }

    @Override
    public void setDone() {
        // ignored, since this end of the queue is input only
    }
    
    public static <U extends Image> RoundRobinInputQueue<U> create(
            Class<U> type, String name, int max, int outputs) {
        
        // Not sure if this works....
        return new RoundRobinInputQueue<U>(name, max, outputs);
    }

    @Override
    public void printStats() {
    
        for (PutOnlyQueue<?> q : queues) { 
            q.printStats();
        }
        
    }
}

