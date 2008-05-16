package processors;

import java.util.HashMap;

import util.Options;
import util.config.ComponentDescription;

import image.CompressedImage;
import image.ImageCompressor;
import image.ImageQueue;
import image.UncompressedImage;
import image.compression.CompressorFactory;

public class Compressor extends ImageProcessor<UncompressedImage, CompressedImage> {
    
    private final ImageCompressor compressor;
    
    public Compressor(int componentNumber, String name, ImageQueue<UncompressedImage> in, 
            ImageQueue<CompressedImage> out, ImageCompressor comp, 
            StatisticsCallback publisher) {

        // Store the name and in/output queues
        super(componentNumber, "Compressor", name, in, out, publisher);
        this.compressor = comp;
    }
    
    @Override
    public void process() { 

        // Note: we need these field to store the processing time and input data
        //       sizes until the compressor produces an output image (with 
        //       video compression this may only occur once every X frames).  
        long sizeIn = 0;
        long timeIn = 0;
        
        UncompressedImage i = in.get();

        while (i != null) {
        
            // logger.warn("Got image " + i.number);
        
            try { 
                long t1 = System.currentTimeMillis();

                CompressedImage c = compressor.addImage(i);
                
                long t2 = System.currentTimeMillis();

                sizeIn += i.getSize();
                timeIn += t2-t1;
                
                if (c != null) { 
                    processedImage(c.number, timeIn, sizeIn, c.getSize(), out.putMayBlock());
                    sizeIn = timeIn = 0;
                    out.put(c);
                } 
                
            } catch (Exception e) { 
                logger.warn("Failed to handle " + i.number, e);
                e.printStackTrace();
            }

            i = in.get();
        }
        
        try { 
            long t1 = System.currentTimeMillis();

            CompressedImage c = compressor.flush();

            long t2 = System.currentTimeMillis();

            if (c != null) { 
                processedImage(c.number, timeIn + (t2-t1), sizeIn, c.getSize(), out.putMayBlock());
                out.put(c);
            }

        } catch (Exception e) { 
            logger.warn("Failed to flush", e);
        }
            
        out.setDone();
    }
    
    public static Class<UncompressedImage> getInputQueueType() {
        return UncompressedImage.class;
    }

    @SuppressWarnings("unchecked")
    public static Class<CompressedImage> getOutputQueueType() {
        return CompressedImage.class;
    }   
    
    public static Compressor create(ComponentDescription c,
            ImageQueue<UncompressedImage> in, ImageQueue<CompressedImage> out, 
            StatisticsCallback publisher) throws Exception {
        
        HashMap<String, String> options = c.getOptions();
        
        String type = Options.getStringOption(options, "type");
   
        ImageCompressor comp = CompressorFactory.create(type, options);
        
        return new Compressor(c.getNumber(), c.getName(), in, out, comp, publisher);
    }
    
}
