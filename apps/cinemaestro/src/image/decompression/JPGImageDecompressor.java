package image.decompression;

import image.CompressedImage;
import image.Convert;
import image.ImageDecompressor;
import image.UncompressedImage;

import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.io.ByteArrayInputStream;
import java.util.Iterator;

import javax.imageio.IIOException;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.plugins.jpeg.JPEGImageReadParam;
import javax.imageio.stream.ImageInputStream;

public class JPGImageDecompressor implements ImageDecompressor {
    
    // private final Logger logger = Logger.getLogger("Decompressor.JPG");
    
    private JPEGImageReadParam readParam;
    private ImageReader reader;
    
    public JPGImageDecompressor() throws IIOException { 
        // Get a TIFF reader and set its input to the written TIFF image.
        ImageIO.setUseCache(false);
        
        Iterator readers = ImageIO.getImageReadersByFormatName("jpg");

        if (readers == null || !readers.hasNext()) {
            throw new RuntimeException("No readers!");
        }

        reader = (ImageReader) readers.next();

        // Create the read param.
        readParam = new JPEGImageReadParam();
    }

    public String getType() {
        return "JPG";
    }
    
    public UncompressedImage decompress(CompressedImage cim) throws Exception { 

        if (!cim.compression.equals("JPG")) { 
            throw new Exception("JPG Decompressor cannot handle " + cim.compression);
        }
        
        ByteArrayInputStream in = 
            new ByteArrayInputStream((byte[]) cim.getData());
        
        ImageInputStream input = ImageIO.createImageInputStream(in);
        
        if (input == null) { 
            System.err.println("Input = " + input);
            System.exit(1);
        }

        reader.setInput(input);
        
        // Read the image.
        BufferedImage image = reader.read(0, readParam);
        Raster r = image.getData();
        
        return Convert.toRGBImage(cim.number, cim.metaData, r);
    }
}
