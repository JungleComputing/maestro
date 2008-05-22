package ibis.videoplayer;

import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferUShort;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;

abstract class UncompressedImage extends Image {

    UncompressedImage(int width, int height, int frameno) {
        super(width, height, frameno);
    }

    abstract IIOImage toIIOImage();

    /**
     * Given a file that we know is a PNG file, load it, and return
     * and uncompressed image for it.
     * @param f The file to load.
     * @return The image of that file.
     * @throws IOException Thrown if the file cannot be read.
     */
    static UncompressedImage loadPNG( File f, int frameno ) throws IOException
    {
        BufferedImage image = ImageIO.read( f );
        SampleModel sm = image.getSampleModel();
        final int width = sm.getWidth();
        final int height = sm.getHeight();
        WritableRaster raster = Raster.createBandedRaster( DataBuffer.TYPE_USHORT, width, height, sm.getNumBands(), null );
        raster = image.copyData( raster );
        DataBuffer buffer = raster.getDataBuffer();
        if( buffer instanceof DataBufferUShort ){
            DataBufferUShort sb = (DataBufferUShort) buffer;
            
            short banks[][] = sb.getBankData();
            return new RGB48Image( frameno, width, height, banks[0], banks[1], banks[2] );
        }
        return null;
    }

}