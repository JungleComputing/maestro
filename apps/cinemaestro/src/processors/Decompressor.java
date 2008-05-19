package processors;

import image.CompressedImage;
import image.ImageDecompressor;
import image.ImageQueue;
import image.UncompressedImage;
import image.decompression.DecompressorFactory;

import java.util.HashMap;

import util.config.ComponentDescription;

public class Decompressor extends ImageProcessor<CompressedImage, UncompressedImage> {

    private final HashMap<String, ImageDecompressor> decomp = 
        new HashMap<String, ImageDecompressor>();
    
    private final DecompressorFactory factory;
  
    public Decompressor(int componentNumber, String name, ImageQueue<CompressedImage> in, 
            ImageQueue<UncompressedImage> out, ImageDecompressor [] dec, 
            StatisticsCallback publisher) {

        // Store the name and in/output queues
        super(componentNumber, "Decompressor", name, in, out, publisher);
    
        factory = null;
        
        for (ImageDecompressor id : dec) { 
            decomp.put(id.getType(), id);
        }
    }
    
    public Decompressor(int componentNumber, String name, ImageQueue<CompressedImage> in, 
            ImageQueue<UncompressedImage> out, DecompressorFactory factory, 
            StatisticsCallback publisher) {

        // Store the name and in/output queues
        super(componentNumber, "Decompressor", name, in, out, publisher);
        
        this.factory = factory; 
    }
    
    private ImageDecompressor getDecompressor(String type) {
        
        ImageDecompressor d = decomp.get(type);
        
        if (d == null && factory != null) { 
            
            try { 
                d = factory.create(type);
            } catch (Exception e) {
                // ignore
            }
                
            if (d != null) { 
                decomp.put(type, d);
            }
        }
        
        return d;
    }
    
    @Override
    public void process() { 
        
        CompressedImage c = in.get();

        while (c != null) {
            
            ImageDecompressor d = getDecompressor(c.compression);

            if (d != null) {

                try { 
                    long t1 = System.currentTimeMillis();

                    UncompressedImage u = d.decompress(c);
                    
                    long t2 = System.currentTimeMillis();

                    processedImage(c.number, t2-t1, c.getSize(), u.getSize(), 
                            out.putMayBlock());
                    
                    out.put(u);    
              
                } catch (Exception e) { 
                    logger.warn("Failed to handle " + c.number + " ("
                            + c.compression + ")", e);     
                }
            } else { 
                logger.warn("Don't know how to handle " + c.number + " ("
                        + c.compression + ")");
            } 

            c = in.get();
        }

        out.setDone();
    }
    
    @SuppressWarnings("unchecked")
    public static Class getInputQueueType() {
        return CompressedImage.class;
    }

    @SuppressWarnings("unchecked")
    public static Class<UncompressedImage> getOutputQueueType() {
        return UncompressedImage.class;
    }    
    
    public static Decompressor create(ComponentDescription c,
            ImageQueue<CompressedImage> in, ImageQueue<UncompressedImage> out, 
            StatisticsCallback publisher) 
        throws Exception {
        
        return new Decompressor(c.getNumber(), c.getName(), in, out, 
                new DecompressorFactory(c.getOptions()), publisher);
    }
    
}
