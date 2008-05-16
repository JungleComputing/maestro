package image.compression;

import image.CompressedImage;
import image.Convert;
import image.ImageCompressor;
import image.TIFCompressedImage;
import image.UncompressedImage;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;

import org.apache.log4j.Logger;

public class TIFCompressor implements ImageCompressor {
    
    private final Logger logger = Logger.getLogger("Compressor.TIF");
    
    private ImageWriter writer;
  //  private TIFFImageWriteParam writeParam;
    
    public TIFCompressor() { 
    
        // Get a TIFF reader and set its input to the written TIFF image.
        ImageIO.setUseCache(false);
        
        Iterator writers = ImageIO.getImageWritersByFormatName("tif");

        if (writers == null || !writers.hasNext()) {
            throw new RuntimeException("No writers!");
        }

        writer = (ImageWriter) writers.next();
        
        // Create the write param.
    //    writeParam = new TIFFImageWriteParam(null);
        //writeParam.setCompressionQuality(90);
    }
    
    public CompressedImage addImage(UncompressedImage image) throws Exception {

        BufferedImage b = new BufferedImage(image.width, image.height, BufferedImage.TYPE_3BYTE_BGR);
        b.getRaster().setRect(Convert.toRaster(image));
        
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        
        ImageOutputStream output = ImageIO.createImageOutputStream(out);
        
        writer.setOutput(output);
        writer.write(b);
        
        output.close();
        
        byte [] tmp = out.toByteArray();
        
        logger.info("Compression resulted in " + tmp.length + " bytes.");
        
        return new TIFCompressedImage(image.number, image.metaData, tmp); 
    }

    public CompressedImage flush() throws Exception {
        // Nothing to do here...
        return null;
    }

    public String getType() {
        return "TIF";
    }
}
