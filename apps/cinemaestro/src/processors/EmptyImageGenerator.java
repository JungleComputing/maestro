package processors;

import image.Image;
import image.ImageQueue;
import image.RGB24Image;
import image.RGB48Image;
import image.UncompressedImage;

import java.util.HashMap;

import util.Options;
import util.config.ComponentDescription;

public class EmptyImageGenerator extends ImageProducer<UncompressedImage> {
    
    private int images;
    private int rank;
    private int size;
    private int w; 
    private int h;
    private int bits;
        
    private long frameGap = 0;
    
    public EmptyImageGenerator(int componentNumber, String name, 
            ImageQueue<UncompressedImage> out, int w, int h, int images, 
            int bits, int rank, int size, int fps, StatisticsCallback publisher) 
        throws Exception {
        
        super(componentNumber, "EmptyImageGenerator", name, out, publisher);
    
        this.w = w;
        this.h = h; 
        this.bits = bits;
        this.images = images;
        this.rank = rank;
        this.size = size;
        
        if (fps > 0) { 
            frameGap = (1000*size) / fps; 
        }        
    }

    private UncompressedImage generate(int number) throws Exception { 
        
        int num = rank + number * size;
        
        if (bits == 24) { 
            return new RGB24Image(num, w, h, null, new byte[w*h*3]);
        } else if (bits == 48) {
            return new RGB48Image(num, w, h, null, new short[w*h*3]);            
        } else { 
            throw new Exception("Unsupported image type!");
        }  
    }
    
    public void process() {

        long last = 0;

        System.out.println("[*] GENERATOR TARGET GAP " + frameGap);
        
        for (int i=0;i<images;i++) { 
            try { 
                long t1 = System.currentTimeMillis();

                UncompressedImage c = generate(i);

                long t2 = System.currentTimeMillis();

                processedImage(c.number, t2-t1, c.getSize(), c.getSize(), 
                        out.putMayBlock());
                
                if (frameGap > 0) { 
                    if (last > 0) { 
                        // Lets produce the frames 3 ms. early, just to be sure
                        long sleep = frameGap - (System.currentTimeMillis() - last) - 3;
                        
                        if (sleep > 0) { 
                            Thread.sleep(sleep);
                        } else { 
                            System.out.println("[*] GENERATOR MISSED DEADLINE! " + sleep);
                        }
                    }

                    last = System.currentTimeMillis();
                } 

                out.put(c);
            } catch (Exception e) {
                System.out.println("[*] EEP: " + e);
            }
        }
        
        out.setDone();
    }

    public static Class<? extends Image> getInputQueueType() { 
        return null;
    }
    
    public static Class<UncompressedImage> getOutputQueueType() {
        return UncompressedImage.class;
    }    
    
    public static EmptyImageGenerator create(ComponentDescription c, 
            ImageQueue in, ImageQueue<UncompressedImage> out,
            StatisticsCallback publisher) throws Exception {
   
        HashMap<String, String> options = c.getOptions();
        
        int w = Options.getIntOption(options, "width");
        int h = Options.getIntOption(options, "height");
        int frames = Options.getIntOption(options, "frames");
        int bits = Options.getIntOption(options, "bits");
        
        int fps = Options.getIntOption(options, "fps", false, 0);
                       
        return new EmptyImageGenerator(c.getNumber(), c.getName(), out, 
                w, h, frames, bits, c.getRank(), c.getSize(), fps, publisher);
    }
    
}
