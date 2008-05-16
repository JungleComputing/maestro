package image;

public abstract class YUVImage extends UncompressedImage {

    public YUVImage(long number, int width, int height, Object metaData) {
        super(number, width, height, metaData);
    }

    public abstract RGBImage toRGB();
}

