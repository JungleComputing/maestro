package ibis.videoplayer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;


class JPEGCompressor
{    
   // private JPEGImageWriteParam writeParam;
    
    /** FIXME. (Overrides method in superclass.)
     * @param image The uncompressed image.
     * @return The compressed image.
     * @throws IOException 
     */
    public static CompressedImage compress(UncompressedImage image) throws IOException
    {
        ImageWriter writer;
        IIOImage b = image.toIIOImage();
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        ImageIO.setUseCache( false );
        ImageOutputStream output = ImageIO.createImageOutputStream(out);
        Iterator<?> writers = ImageIO.getImageWritersByFormatName( "jpg" );

        if( writers == null || !writers.hasNext() ){
            throw new RuntimeException( "No jpeg writers!" );
        }

        writer = (ImageWriter) writers.next();
        writer.setOutput( output );
        writer.write( b );
        writer.dispose();

        byte [] tmp = out.toByteArray();

      //  System.out.println("Compression " + image.width + "x" + image.height + " from " + image.getSize() + " to " + tmp.length + " bytes.");
        
        return new JpegCompressedImage( image.width, image.height, image.frameno, tmp ); 
    }
}
