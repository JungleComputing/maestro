package ibis.videoplayer;

import javax.imageio.IIOImage;

abstract class UncompressedImage extends Image {

    UncompressedImage(int width, int height, int frameno) {
        super(width, height, frameno);
    }

    abstract IIOImage toIIOImage();

}