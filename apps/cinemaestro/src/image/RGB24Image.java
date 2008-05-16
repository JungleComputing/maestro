package image;

public class RGB24Image extends RGBImage {

    /** 
     * Generated
     */
    private static final long serialVersionUID = -7495717832792906499L;
  
    private byte [] data;
    
    public RGB24Image(long number, int width, int height, Object metaData, byte [] data) { 
        super(number, width, height, metaData);
        this.data = data;
    }

    @Override
    public Object getData() {
        return data;
    }

    @Override
    public UncompressedImage scale(int targetW, int targetH) {
        
        if (targetW == width && targetH == height) { 
            return new RGB24Image(number, targetW, targetH, metaData, data.clone());
        }
        
        // Do a simple interpolation here. Not necessarily a decent algorithm!
        byte [] tmp = new byte[targetW*targetH*3];
        
        double multX = ((double) width) / targetW;
        double multY = ((double) height) / targetH;
        
        for (int h=0;h<targetH;h++) { 
            
            final int off = 3 * h * targetW;
            final int origOff = (int) (3 * (h*multY) * (targetW * multX));
            
            for (int w=0;w<targetW;w++) {
                
                final int origW = (int) (w * multX) * 3;
                
                tmp[off + w*3 + 0] = data[origOff + origW + 0];
                tmp[off + w*3 + 1] = data[origOff + origW + 1];
                tmp[off + w*3 + 2] = data[origOff + origW + 2];
                
            }   
        }
        
        return new RGB24Image(number, targetW, targetH, metaData, tmp);
    }

    @Override
    public YUVImage toYUV() {
       
        byte [] Y = new byte[width*height];
        byte [] U = new byte[width*height/4];
        byte [] V = new byte[width*height/4];
        
        Convert.RGB24toYUV24(data, Y, U, V, width, height);
        
        return new YUV24Image(number, width, height, metaData, Y, U, V);
    }
    
    @Override
    public long getSize() { 
        return data.length;
    }

    @Override
    public void colorAdjust(double r, double g, double b) {
        for (int i=0;i<data.length;i+=3) { 
            data[i+0] = (byte) (data[i+0] * r);
            data[i+1] = (byte) (data[i+1] * g);
            data[i+2] = (byte) (data[i+2] * b);
        }
    }

    @Override
    public UncompressedImage scale(int width, int height, int bits) {
        throw new RuntimeException("Not implemented!");
    }
}
