package ibis.videoplayer;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;


public class JPEGCompressor implements ImageCompressor {    
    private ImageWriter writer;
   // private JPEGImageWriteParam writeParam;
    
    public JPEGCompressor() { 
    
        //      Get a TIFF reader and set its input to the written TIFF image.
             
        // Create the write param.
   //     writeParam = new JPEGImageWriteParam(null);
        //writeParam.setCompressionQuality(90);
    }
    
    public CompressedImage addImage(UncompressedImage image) throws Exception {

        BufferedImage b = image.toBufferedImage();
      
        ByteArrayOutputStream out = new ByteArrayOutputStream();
     
        ImageIO.setUseCache(false);
        
        ImageOutputStream output = ImageIO.createImageOutputStream(out);
        
        Iterator<?> writers = ImageIO.getImageWritersByFormatName("jpg");

        if (writers == null || !writers.hasNext()) {
            throw new RuntimeException("No writers!");
        }

        writer = (ImageWriter) writers.next();
        writer.setOutput(output);
        writer.write(b);
        writer.dispose();
        
        byte [] tmp = out.toByteArray();

      //  System.out.println("Compression " + image.width + "x" + image.height + " from " + image.getSize() + " to " + tmp.length + " bytes.");
        
        return new JPGCompressedImage( image.width, image.height, image.frameno, tmp ); 
    }

    public CompressedImage flush() throws Exception {
        // Nothing to do here...
        return null;
    }

    public String getType() {
        return "JPG";
    }
}
