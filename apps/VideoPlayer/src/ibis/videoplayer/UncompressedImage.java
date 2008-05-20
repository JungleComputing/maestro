package ibis.videoplayer;

import java.awt.image.BufferedImage;

abstract class UncompressedImage extends Image {

    UncompressedImage(int width, int height, int frameno) {
        super(width, height, frameno);
    }

    abstract BufferedImage toBufferedImage();

}