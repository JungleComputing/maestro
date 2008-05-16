package test;

import image.Convert;
import image.RGB48Image;
import image.RGBImage;
import image.decompression.MYTIFFLZWDecompressor;

import java.awt.image.BufferedImage;
import java.awt.image.PixelInterleavedSampleModel;
import java.awt.image.Raster;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;

import com.sun.media.imageio.plugins.tiff.BaselineTIFFTagSet;
import com.sun.media.imageio.plugins.tiff.TIFFImageReadParam;
import com.sun.media.imageio.plugins.tiff.TIFFImageWriteParam;

public class TIF2TIF {

    private static byte [] readFully(File f) throws IOException { 

        byte [] tmp = new byte[(int) f.length()];

        FileInputStream in = new FileInputStream(f);

        int read = in.read(tmp);

        if (read == -1) { 
            throw new EOFException();
        }

        in.close();

        return tmp;        
    }

    public static void main(String [] args) throws IOException { 

        File f = new File(args[0]);

        System.out.println("File: " + f.getName()); 

      //  long t1 = System.currentTimeMillis();

        // Get a TIFF reader and set its input to the written TIFF image.
        Iterator readers = ImageIO.getImageReadersByFormatName("tif");

        if(readers == null || !readers.hasNext()) {
            throw new RuntimeException("No readers for " + f);
        }

        ImageReader reader = (ImageReader) readers.next();

        // BufferedInputStream in = new BufferedInputStream(new FileInputStream(f));

        byte [] tmp = readFully(f);                

     //   long t2 = System.currentTimeMillis();

        ImageIO.setUseCache(false);

        ImageInputStream input = ImageIO.createImageInputStream(new ByteArrayInputStream(tmp));

        if (input == null) { 
            System.err.println("Input = " + input);
            System.exit(1);
        }

        reader.setInput(input);

        MYTIFFLZWDecompressor dec = new MYTIFFLZWDecompressor(
                BaselineTIFFTagSet.PREDICTOR_HORIZONTAL_DIFFERENCING);

        // Create the read param.
        TIFFImageReadParam readParam = new TIFFImageReadParam();
        readParam.setTIFFDecompressor(dec);

        // Read the image.
        BufferedImage image = reader.read(0, readParam);

        System.out.println("Decoded size " + dec.bytesOut);

        int type = image.getType();
        
        System.out.println("Type: " + type);

        Raster r = image.getData();
        
        PixelInterleavedSampleModel sm = (PixelInterleavedSampleModel) r.getSampleModel();

        System.out.println("SampleModel: " + sm.getClass());
        System.out.println("  - " + sm.getWidth() + " x " + sm.getHeight() );
        System.out.println("  - " + sm.getNumBands());
        System.out.println("  - " + sm.getPixelStride());
        System.out.println("  - " + sm.getScanlineStride());
        System.out.println("  - " + Arrays.toString(sm.getBandOffsets()));

        //long t3 = System.currentTimeMillis();
        
        RGBImage rgb = Convert.toRGBImage(0, null, r);
        
        if (rgb instanceof RGB48Image) { 
            rgb = Convert.toRGB24((RGB48Image) rgb);
        }
        
        Raster r2 = Convert.toRaster(rgb);
        
        BufferedImage b = new BufferedImage(rgb.width, rgb.height, BufferedImage.TYPE_3BYTE_BGR);
        b.getRaster().setRect(r2);
        
        
        
        type = b.getType();
        
        System.out.println("Type: " + type);

        
        Iterator writers = ImageIO.getImageWritersByFormatName("tiff");

        if(writers == null || !writers.hasNext()) {
            throw new RuntimeException("No writers for tiff");
        }

        ImageWriter writer = (ImageWriter)writers.next();

        ImageOutputStream output =
            ImageIO.createImageOutputStream(new File(f.getName() + ".out.tif"));

        writer.setOutput(output);

        // Create the write param.
        TIFFImageWriteParam writeParam = new TIFFImageWriteParam(null);
      //  writeParam.setCompressionMode(writeParam.MODE_EXPLICIT);
      //  writeParam.setTIFFCompressor(new TIFFLZWCompressor(BaselineTIFFTagSet.PREDICTOR_NONE));
        //writeParam.setCompressionType(compressor.getCompressionType());

        // Write the image.
        writer.write(null, new IIOImage(b, null, null), writeParam);
    }

  
}
