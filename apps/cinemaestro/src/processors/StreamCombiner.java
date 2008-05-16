package processors;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.HashMap;

import util.Options;
import util.config.ComponentDescription;

import image.CompressedImage;
import image.ImageQueue;

public class StreamCombiner extends ImageConsumer<CompressedImage> {
    
    private final OutputStream out;
    
    public StreamCombiner(int componentNumber, String name, ImageQueue<CompressedImage> in, 
            OutputStream out, StatisticsCallback publisher) {
        super(componentNumber, "StreamCombiner", name, in, publisher);
        
        this.out = out;
    }
    
    public void process() { 

        CompressedImage i = in.get();

        long next = i.number;
        
        while (i != null) {

            // Santity check 
            if (next != i.number) { 
                logger.warn("Got image " + i.number + " expected " + next);
            }
            
            long t1 = System.currentTimeMillis();
            
            try {
                out.write((byte[]) i.getData());
                out.flush();
            } catch (Exception e) { 
                logger.warn("Failed to write " + i.number, e);
            }

            long t2 = System.currentTimeMillis();
            
            processedImage(i.number, t2-t1, i.getSize(), i.getSize(), in.getMayBlock());
            
            next++;
            
            i = in.get();
        }
    }
    
    @SuppressWarnings("unchecked")
    public static Class getInputQueueType() {
        return CompressedImage.class;
    }
    
    public static Class getOutputQueueType() {
        return null;
    }
    
    public static StreamCombiner create(ComponentDescription c,
            ImageQueue<CompressedImage> in, ImageQueue out, 
            StatisticsCallback publisher) 
        throws Exception {
   
        HashMap<String, String> options = c.getOptions();
        
        String output = Options.getStringOption(options, "output");
        
        OutputStream o = null;
        
        if (output.equals("stdout")) { 
            o = System.out;
        } else if (output.startsWith("file://")) {
            
            String filename = output.substring(7).trim();
            
            if (filename.length() == 0) { 
                throw new Exception("Illegal filename!");
            }
            
            o = new BufferedOutputStream(new FileOutputStream(filename));
        } else { 
            throw new Exception("Unknown output: " + output);
        }
        
        return new StreamCombiner(c.getNumber(), c.getName(), in, o, publisher);
    }
}

