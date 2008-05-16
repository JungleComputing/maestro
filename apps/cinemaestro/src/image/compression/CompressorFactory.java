package image.compression;

import java.util.HashMap;

import util.Options;

import image.ImageCompressor;

public class CompressorFactory {
    
    public static ImageCompressor create(String type, HashMap<String, String> options) throws Exception { 
        
        if (type.equals("JPG")) { 
            return new JPEGCompressor();
        } else if (type.equals("TIF")) { 
            return new TIFCompressor();
        } else if (type.equals("H264")) {
            
            int w = Options.getIntOption(options, "width");
            int h = Options.getIntOption(options, "height");
            
            int framesPerBlock = Options.getIntOption(options, "framesPerBlock", false, 30);
            int bitrate = Options.getIntOption(options, "bitrate", false, 20000);
             
            return new H264Compressor(w, h, framesPerBlock, bitrate);
       
        } else if (type.equals("NULL")) {
            return new NullCompressor();
        }
        
        throw new Exception("Unknown compressor " + type);        
    }
}
