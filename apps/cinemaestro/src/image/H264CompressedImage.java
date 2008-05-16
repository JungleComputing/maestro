package image;

public class H264CompressedImage extends CompressedImage {

    /** 
     * Generated
     */
    private static final long serialVersionUID = -3300212587094539560L;
  
    private final byte [] data;
    private final int numberOfFrames;
    
    public H264CompressedImage(long number, Object metaData, byte [] data, int frames) {
        super(number, metaData, "H264Compressor");
        this.data = data;
        this.numberOfFrames = frames;
    }

    @Override
    public Object getData() {
        return data;
    }

    public int getNumberOfFrames() { 
        return numberOfFrames;
    }
    
    @Override
    public long getSize() { 
        return data.length;
    }
}
