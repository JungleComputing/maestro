package processors;

import image.Convert;
import image.ImageQueue;
import image.RGB48Image;
import image.UncompressedImage;
import util.config.ComponentDescription;

public class ToRGB24 extends ImageProcessor<UncompressedImage, UncompressedImage> {
    
    public ToRGB24(int componentNumber, String name, 
            ImageQueue<UncompressedImage> in, ImageQueue<UncompressedImage> out,
            StatisticsCallback publisher) {

        // Store the name and in/output queues
        super(componentNumber, "ToRGB24", name, in, out, publisher);
    }
    
    @Override
    public void process() { 

        UncompressedImage i = in.get();

        while (i != null) {
            try { 
                long t1 = System.currentTimeMillis();

                if (i instanceof RGB48Image) { 
                    i = Convert.toRGB24((RGB48Image) i);
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
    
    public static Class<UncompressedImage> getInputQueueType() {
        return UncompressedImage.class;
    }

    public static Class<UncompressedImage> getOutputQueueType() {
        return UncompressedImage.class;
    }    
    
    public static ToRGB24 create(ComponentDescription c,
            ImageQueue<UncompressedImage> in, ImageQueue<UncompressedImage> out, 
            StatisticsCallback publisher) 
        throws Exception {
        
        return new ToRGB24(c.getNumber(), c.getName(), in, out, publisher);
    }
    
    
}
