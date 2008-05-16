package processors;

import java.util.HashMap;

import util.Options;
import util.config.ComponentDescription;

import image.Image;
import image.ImageQueue;
import image.UncompressedImage;

public class ColorConvertor extends ImageProcessor<UncompressedImage, UncompressedImage> {
    
    private final double r;
    private final double g;
    private final double b;
    
    public ColorConvertor(int componentNumber, String name, 
            ImageQueue<UncompressedImage> in, ImageQueue<UncompressedImage> out,
            double r, double g, double b, StatisticsCallback publisher) {

        // Store the name and in/output queues
        super(componentNumber, "ColorConvertor", name, in, out, publisher);
     
        this.r = r;
        this.g = g;
        this.b = b;
    }
    
    public void process() { 

        UncompressedImage i = in.get();

        while (i != null) {
            try { 
                long t1 = System.currentTimeMillis();

                i.colorAdjust(r, g, b);
                
                long t2 = System.currentTimeMillis();
                
                processedImage(i.number, t2-t1, i.getSize(), i.getSize(), out.putMayBlock());
                
                out.put(i);
                
            } catch (Exception e) { 
                logger.warn("[*] Failed to handle " + i.number, e);
            }

            i = in.get();
        }

        out.setDone();
    }
    
    public static Class<Image> getInputQueueType() {
        return UncompressedImage.class;
    }

    public static Class<Image> getOutputQueueType() {
        return UncompressedImage.class;
    }    
    
    public static ColorConvertor create(ComponentDescription c,
            ImageQueue<UncompressedImage> in, ImageQueue<UncompressedImage> out, 
            StatisticsCallback publisher) 
        throws Exception {
        
        HashMap<String, String> options = c.getOptions();
        
        double r = Options.getDoubleOption(options, "red", false, 1.0);
        double g = Options.getDoubleOption(options, "green", false, 1.0);
        double b = Options.getDoubleOption(options, "blue", false, 1.0);
        
        return new ColorConvertor(c.getNumber(), c.getName(), in, out, r, g, b, publisher);
    }
    
    
}
