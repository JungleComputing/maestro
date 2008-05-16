package processors;

import image.Image;

import java.util.HashMap;

import org.apache.log4j.Logger;

public abstract class ProcessorThread extends Thread {

    private static final int MIN_PUBLISH_DELAY = 100000;
    private static final int MAX_PUBLISH_DELAY = 1000000;
    
    protected final Logger logger; 
    
    protected final String type;
    protected final String name;
    protected final int componentNumber;
    
    private long totalImages = -1;
    
    protected Statistics stats;
    
    private StatisticsCallback publisher; 
    
    private boolean done = false;
    
    private long lastPublish = -1;
    
    /**
     * Construct a new ImageProcessor
     * 
     * @param logger
     * @param type
     * @param name
     */
    public ProcessorThread(final int componentNumber, final String type, 
            final String name, final StatisticsCallback publisher) {
        this.componentNumber = componentNumber;
        this.type = type;
        this.name = name;
        this.publisher = publisher;
        
        logger = Logger.getLogger(type + "." + name);
    }
    
    public ProcessorThread(final int componentNumber, final String type, 
            final String name) {
        
        this(componentNumber, type, name, null);
    }
    
    public void processedImage(long number, long time, long sizeIn, long sizeOut, boolean publish) {
    
        stats.add(System.currentTimeMillis(), time, sizeIn, sizeOut);

	// System.out.println(name + " processed image " + number + ": " + time + " " + sizeIn + " " + sizeOut);
        
/*      if (publisher != null) { 
            long now = System.currentTimeMillis();

            if (publish && MIN_PUBLISH_DELAY < (now-lastPublish)) {
                lastPublish = now;
                publisher.publish(stats);
                
            } else if (lastPublish == -1 || 
                    (MAX_PUBLISH_DELAY < (now - lastPublish))) {
                
                lastPublish = now;
                publisher.publish(stats);                
            }
        }
        */
    }
    
    public synchronized long getProcessedImage() {
        
        if (stats != null) { 
            return stats.getImages();
        } else { 
            return 0;
        }
    } 
    
    public synchronized void setTotalImages(long total) { 
        totalImages = total;
        
        System.out.println("[*] SET TOTAL TO " + total);
    } 
    
    public synchronized boolean getDone() { 
        return done;
    } 
    
    public abstract void process() throws Exception;
    
    public static Class<? extends Image> getInputQueueType() { 
        return null;
    }
    
    public static Class<? extends Image> getOutputQueueType() {
        return null;
    }
   
    public synchronized void printStatistics() { 
        stats.printStatistics();
    }
    
    public void run() { 
        
        stats = new Statistics(componentNumber, name, type, 
                System.currentTimeMillis(), totalImages);
        
        logger.info("Started!");
        
        try {
            process();
        } catch (Exception e) {
            logger.warn("Processing failed!", e);
        }

        stats.done(System.currentTimeMillis());
   
        if (publisher != null) { 
            publisher.publish(stats);
        }
        
        synchronized (this) { 
            done = true;
        }
    }
    
    public static Object [] getOptions(Class [] types, String [] names, 
            boolean [] required, HashMap<String, String> input) throws Exception { 
        
        Object [] result = new Object[names.length];
        
        for (int i=0;i<names.length;i++) {
            
            String value = input.get(names[i]);
            
            if (value==null && required[i]) { 
                throw new Exception("Option " + names[i] + " missing!");
            }
            
            if (types[i].equals(int.class)) { 
                result[i] = Integer.parseInt(value);
            } else if (types[i].equals(String.class)) { 
                result[i] = value;
            } else { 
                throw new Exception("Unknown parameter type!");
            }
        }
        
        return result;
    }
    
}
