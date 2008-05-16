package image.decompression;

import image.CompressedImage;
import image.Convert;
import image.ImageDecompressor;
import image.UncompressedImage;

import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Iterator;

import javax.imageio.IIOException;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import org.apache.log4j.Logger;

import com.sun.media.imageio.plugins.tiff.BaselineTIFFTagSet;
import com.sun.media.imageio.plugins.tiff.TIFFImageReadParam;

public class TIFImageDecompressor implements ImageDecompressor {
   
    private final Logger logger = Logger.getLogger("Decompressor.TIF");
    
    public TIFImageDecompressor() throws IIOException { 
        // empty
    }

    public String getType() {
        return "TIF";
    }
      
    private UncompressedImage decodeStandard(CompressedImage cim, InputStream in) throws Exception { 

        // Get a TIFF reader and set its input to the written TIFF image.
        ImageIO.setUseCache(false);
        
        Iterator readers = ImageIO.getImageReadersByFormatName("tif");

        if (readers == null || !readers.hasNext()) {
            throw new RuntimeException("No readers!");
        }

        ImageReader reader = (ImageReader) readers.next();
        
        ImageInputStream input = ImageIO.createImageInputStream(in);
        
        if (input == null) { 
            throw new Exception("Failed to create input!");
        }

        reader.setInput(input);

        // Create the read param.
        TIFFImageReadParam readParam = new TIFFImageReadParam();
        
        // Read the image.
        BufferedImage image = reader.read(0, readParam);

        input.close();
        
        // Tell the reader to clean up the used resources!
        reader.dispose();
        
        Raster r = image.getData();
        
        return Convert.toRGBImage(cim.number, cim.metaData, r);
    }

    private UncompressedImage decodeAlternative(CompressedImage cim, InputStream in) throws Exception { 
       
        // Get a TIFF reader and set its input to the written TIFF image.
        ImageIO.setUseCache(false);
        
        Iterator readers = ImageIO.getImageReadersByFormatName("tif");

        if (readers == null || !readers.hasNext()) {
            throw new RuntimeException("No readers!");
        }

        ImageReader reader = (ImageReader) readers.next();
        
        ImageInputStream input = ImageIO.createImageInputStream(in);
        
        if (input == null) { 
            throw new Exception("Failed to create input!");
        }

        reader.setInput(input);

        // Create the read param.
        TIFFImageReadParam readParam = new TIFFImageReadParam();
  
        MYTIFFLZWDecompressor dec = new MYTIFFLZWDecompressor(
                BaselineTIFFTagSet.PREDICTOR_HORIZONTAL_DIFFERENCING);
        
        readParam.setTIFFDecompressor(dec);
  
        // Read the image.
        BufferedImage image = reader.read(0, readParam);
        
        input.close();

        reader.dispose();
        
        Raster r = image.getData();
        
        return Convert.toRGBImage(cim.number, cim.metaData, r);
    }

    
    
    public UncompressedImage decompress(CompressedImage cim) throws Exception { 

        if (!cim.compression.equals("TIF")) { 
            throw new Exception("TIF Decompressor cannot handle " + cim.compression);
        }
        
        ByteArrayInputStream in = 
            new ByteArrayInputStream((byte[]) cim.getData());
    
        try { 
            try { 
                return decodeStandard(cim, in);
            } catch (Exception e) { 
                logger.info("Standard TIF decompression failed!");
            }
        
            in.reset();
            
            try { 
                return decodeAlternative(cim, in);
            } catch (Exception e) { 
                logger.info("Alternative TIF decompression failed!");
            }
            
        } finally { 
            in.close();
        }
        
        throw new Exception("Failed to decode TIF!");
    }
}
