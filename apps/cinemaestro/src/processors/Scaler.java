package processors;

import java.util.HashMap;

import util.Options;
import util.config.ComponentDescription;

import image.ImageQueue;
import image.UncompressedImage;

public class Scaler extends ImageProcessor<UncompressedImage, UncompressedImage> {
    
    private final int width;
    private final int height;
    private final int bits;
    
    public Scaler(int componentNumber, String name, ImageQueue<UncompressedImage> in, 
            ImageQueue<UncompressedImage> out, int width, int height, int bits,
            StatisticsCallback publisher) {

        // Store the name and in/output queues
        super(componentNumber, "Scaler", name, in, out, publisher);
     
        this.width = width;
        this.height = height;
        this.bits = bits;
    }
    
    public void process() { 

        UncompressedImage i = in.get();

        while (i != null) {
            try { 
                long t1 = System.currentTimeMillis();

                UncompressedImage s = null;
                
                if (bits == 0) { 
                    s = i.scale(width, height);
                } else {
                    s = i.scale(width, height, bits);     
                }
                
                long t2 = System.currentTimeMillis();
                
                processedImage(s.number, t2-t1, i.getSize(), s.getSize(), out.putMayBlock());
                
                out.put(s);
                
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
    
    public static Scaler create(ComponentDescription c,
            ImageQueue<UncompressedImage> in, ImageQueue<UncompressedImage> out, 
            StatisticsCallback publisher) 
        throws Exception {
        
        HashMap<String, String> options = c.getOptions();
        
        int w = Options.getIntOption(options, "width");
        int h = Options.getIntOption(options, "height");
        int bits = Options.getIntOption(options, "bits", false, 0);
        
        return new Scaler(c.getNumber(), c.getName(), in, out, w, h, bits, publisher);
    }
    
    
}
