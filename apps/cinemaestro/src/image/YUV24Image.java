package image;

public class YUV24Image extends YUVImage {

    /** 
     * Generated
     */
    private static final long serialVersionUID = -3396790998330093497L;
    
    private final byte [] Y;
    private final byte [] U;
    private final byte [] V;
    
    /**
     * Construct a new YUV24Image
     * 
     * @param number
     * @param width
     * @param height
     * @param metaData
     */
    public YUV24Image(long number, int width, int height, Object metaData, 
            byte [] Y, byte [] U, byte [] V) {
        super(number, width, height, metaData);
    
        this.Y = Y;
        this.U = U;
        this.V = V;
    }

    @Override
    public Object getData() {
        return new byte [][] { Y, U, V };
    }

    @Override
    public UncompressedImage scale(int w, int h) {
        throw new RuntimeException("Not implemented!");
    }

    @Override
    public RGBImage toRGB() {
        throw new RuntimeException("Not implemented!");
    }
    
    @Override
    public long getSize() { 
        return Y.length + U.length + V.length;
    }

    @Override
    public void colorAdjust(double r, double g, double b) {
        throw new RuntimeException("Not implemented!");
    }

    @Override
    public UncompressedImage scale(int width, int height, int bits) {
        throw new RuntimeException("Not implemented!");
    }
}
