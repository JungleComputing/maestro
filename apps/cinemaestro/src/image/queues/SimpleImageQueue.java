package image.queues;

import image.Image;
import image.ImageQueue;

import java.util.LinkedList;

public class SimpleImageQueue<T extends Image> extends ImageQueue<T> {
    
    private final LinkedList<T> images = new LinkedList<T>();
    
    private int size;
    
    private long totalPutBlock;
    private long totalPutBlockCount;
    private long totalPutCount;
     
    private long totalGetBlock;
    private long totalGetBlockCount;
    private long totalGetCount;
     
    public SimpleImageQueue(String name, int maxImages) { 
        super(name, maxImages);
    }
    
    public SimpleImageQueue(int maxImages) { 
        this("Anonymous", maxImages);
    }
    
    public synchronized T get() {
        
        while (size == 0) {
    
            long start = System.currentTimeMillis();
            
            if (done) { 
                return null;
            }
            
            try { 
                totalGetBlockCount++;
                wait();
            } catch (InterruptedException e) {
                // ignore
            }
            
            long end = System.currentTimeMillis();
         
            totalGetBlock += end-start;
        }
        
        totalGetCount++;
        size--;
        
        notifyAll();        
        return images.removeFirst();
    }

    public synchronized void put(T b) {

        while (size >= maxImages) {
        
            long start = System.currentTimeMillis();
            
            try { 
                totalPutBlockCount++;
                wait();
            } catch (InterruptedException e) {
                // ignore
            }
            
            long end = System.currentTimeMillis();
            
            totalPutBlock += end-start;
         }
      
        totalPutCount++;
        size++;
        
        notifyAll();        
        images.addLast(b);
    }
    
    @Override
    public synchronized boolean getMayBlock() {
        return size == 0;  
    }

    @Override
    public synchronized boolean putMayBlock() {
        return size >= maxImages;  
    }
    
    @Override
    public synchronized int getSize() { 
        return size;
    }
    
    public static <U extends Image> SimpleImageQueue<U> create(Class<U> type, String name, int max) {
        // Not sure if this works....
        return new SimpleImageQueue<U>(name, max);
    }

    @Override
    public void printStats() {
        
        System.out.println("[*] Queue: " + name);
        System.out.println("[*]   len: " + maxImages);

        System.out.println("[*]   put: " + totalPutCount);
        System.out.println("[*] block: " + totalPutBlockCount);
        System.out.println("[*]  time: " + totalPutBlock);
        
        System.out.println("[*]   get: " + totalGetCount);
        System.out.println("[*] block: " + totalGetBlockCount);
        System.out.println("[*]  time: " + totalGetBlock);
        
    }

}

