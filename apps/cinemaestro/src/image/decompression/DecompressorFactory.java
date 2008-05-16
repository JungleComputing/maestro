package image.decompression;

import image.ImageDecompressor;

import java.util.HashMap;

public class DecompressorFactory {

    @SuppressWarnings("unused")
    private final HashMap<String, String> options;
    
    public DecompressorFactory(HashMap<String, String> options) { 
        this.options = new HashMap<String, String>(options);
    }
    
    public ImageDecompressor create(String type) throws Exception { 
        
        if (type.equals("JPG")) { 
            return new JPGImageDecompressor();
        }
        
        throw new Exception("Unknown compressor " + type);        
    }
}
