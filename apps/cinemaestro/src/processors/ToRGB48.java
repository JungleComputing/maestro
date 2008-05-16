package processors;

import java.util.HashMap;

import util.Options;
import util.config.ComponentDescription;

import image.Convert;
import image.ImageQueue;
import image.RGB24Image;
import image.UncompressedImage;

public class ToRGB48 extends ImageProcessor<UncompressedImage, UncompressedImage> {
    
    public ToRGB48(int componentNumber, String name, 
            ImageQueue<UncompressedImage> in, ImageQueue<UncompressedImage> out,
            StatisticsCallback publisher) {

        // Store the name and in/output queues
        super(componentNumber, "ColorConvertor", name, in, out, publisher);
    }
    
    public void process() { 

        UncompressedImage i = in.get();

        while (i != null) {
            try { 
                long t1 = System.currentTimeMillis();

                if (i instanceof RGB24Image) { 
                    i = Convert.toRGB48((RGB24Image) i);
                }
                
                long t2 = System.currentTimeMillis();
                
                processedImage(i.number, t2-t1, i.getSize(), i.getSize(), out.putMayBlock());
                
                out.put(i);
                
            } catch (Exception e) { 
                logger.warn("Failed to handle " + i.number, e);
            }

            i = in.get();
        }

        out.setDone();
    }
    
    public static Class getInputQueueType() {
        return UncompressedImage.class;
    }

    public static Class getOutputQueueType() {
        return UncompressedImage.class;
    }    
    
    public static ToRGB48 create(ComponentDescription c,
            ImageQueue<UncompressedImage> in, ImageQueue<UncompressedImage> out, 
            StatisticsCallback publisher) 
        throws Exception {
        
        return new ToRGB48(c.getNumber(), c.getName(), in, out, publisher);
    }
    
    
}
