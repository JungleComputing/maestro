package image.compression;

import org.apache.log4j.Logger;

import compression.BlockOutput;
import compression.x264;

import image.CompressedImage;
import image.H264CompressedImage;
import image.RGBImage;
import image.ImageCompressor;
import image.UncompressedImage;
import image.YUV24Image;

public class H264Compressor implements ImageCompressor, BlockOutput {
   
    private final Logger logger = Logger.getLogger("Compressor.H264");
    
    private final int framesPerBlock;
   
    private final x264 compressor;
    
    private byte [] current;
    private int currentLen;
    
    private boolean endOfBlock = false;
    
    private long firstNumber = -1;
    private Object firstMeta;
    private int framesInBlock;
    
    public H264Compressor(int width, int height, int framesPerBlock, int bitRate) throws Exception { 
        
        this.framesPerBlock = framesPerBlock;
        
        current = new byte[64*1024];
        
        compressor = new x264(width, height, 100000, bitRate, this);          
    }
    
    public String getType() { 
        return "H264Compressor";
    }
    
    public int getFrames() { 
        return framesPerBlock;
    }
    
    public void store(byte [] buffer, int len) {
        
        logger.info("Storing " + len + " bytes");
        
        if (currentLen + len > current.length) { 
            // resize the current array
            logger.debug("Resize buffer to " + (2*(currentLen + len)) + " bytes");
            
            byte [] tmp = new byte[(currentLen + len) * 2];
            System.arraycopy(current, 0, tmp, 0, currentLen);
            current = tmp;
        }
        
        System.arraycopy(buffer, 0, current, currentLen, len);
        currentLen += len;
    }
    
    public byte[] storeBlock(byte[] buffer, int len, boolean needNew, boolean endOfBlock) {
        
        // System.out.println("Store " + buffer + " " + len + " " + needNew + " " + endOfBlock);
        
        if (buffer != null) { 
            store(buffer, len);
        } 

        if (endOfBlock) { 
            logger.debug("Block done " + currentLen + " bytes");
            this.endOfBlock = true;
        }
        
        if (!needNew) { 
            return null;
        }
        
        return new byte[64*1024];
    }
    
    public CompressedImage addImage(UncompressedImage image) throws Exception {
        
        YUV24Image tmp = null;
        
        // Convert RGB to YUV if needed
        if (image instanceof RGBImage) { 
            image = ((RGBImage) image).toYUV();
        }
        
        // Make sure we have the right type of YUV
        if (image instanceof YUV24Image) { 
            tmp = (YUV24Image) image;
        } else { 
            throw new Exception("Unsupported image type!");
        }
        
        byte [][] data = (byte[][]) tmp.getData();
        compressor.add(data[0], data[1], data[2]);
    
        framesInBlock++;
        
        if (framesInBlock == framesPerBlock) { 
            compressor.flush();
            
            if (!endOfBlock) {
                logger.warn("End of block expected!");
                return null;
            }
            
            if (currentLen == 0) {
                logger.warn("No data at end of block!");
                return null;
            }
            
            byte [] t = new byte[currentLen];
            System.arraycopy(current, 0, t, 0, currentLen);
            
            CompressedImage result = new H264CompressedImage(
                    firstNumber / framesPerBlock, firstMeta, t, framesInBlock);

            currentLen = 0;
            firstNumber = -1;
            firstMeta = null;
            framesInBlock = 0;
         
            logger.info("Produced CompressedImage of size " + result.getSize());
            
            return result;
        
        } else if (firstNumber== -1) { 
                firstNumber = image.number; 
                firstMeta = image.metaData;
        }
        
        return null;
    }

    public CompressedImage flush() throws Exception {

        compressor.flush();
        
        if (currentLen == 0) {
            return null;
        }
        
        byte [] t = new byte[currentLen];
        System.arraycopy(current, 0, t, 0, currentLen);
        
        CompressedImage result = new H264CompressedImage(
                firstNumber / framesPerBlock, 
                firstMeta, t, framesInBlock);

        currentLen = 0;
        firstNumber = -1;
        firstMeta = null;
        framesInBlock = 0;
        
        return result;
    }
}
