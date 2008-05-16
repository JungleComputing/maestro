package test;

import image.CompressedImage;
import image.ImageQueue;
import image.RGB48Image;
import image.UncompressedImage;
import image.compression.TIFCompressor;
import image.queues.SimpleImageQueue;
import processors.CompressedFileWriter;
import processors.Compressor;

public class TestTIF {

    public static void main(String [] args) throws Exception {

        int width = 10;
        int height = 10;

        short [] data = new short[width * height * 3];
        
        int edge = height/2;
        
        for (int h=0;h<edge;h++) { 
            for (int w=0;w<width;w++) { 
                data[3*h*width +3*w] = (short) ((Short.MAX_VALUE * 2) & 0xffff);
                data[3*h*width +3*w+1] = 0; // (short) ((Short.MAX_VALUE * 2) & 0xffff);
                data[3*h*width +3*w+2] = 0; // (short) ((Short.MAX_VALUE * 2) & 0xffff);
            }
        }
     
        for (int h=edge;h<height;h++) { 
            for (int w=0;w<width;w++) { 
                data[3*h*width +3*w] = 0; // (short) ((Short.MAX_VALUE * 2) & 0xffff);
                data[3*h*width +3*w+1] = (short) ((Short.MAX_VALUE * 2) & 0xffff);
                data[3*h*width +3*w+2] = 0; //(short) ((Short.MAX_VALUE * 2) & 0xffff);
            }
        }
        
        for (int h=0;h<height;h++) { 
            for (int w=0;w<width/2;w++) { 
                data[3*h*width +3*w+2] = (short) ((Short.MAX_VALUE * 2) & 0xffff);
            }
        }
        
        
        RGB48Image image = new RGB48Image(0, width, height, null, data);
        
        int number = 0;
        
        // First the output combiner
        ImageQueue<CompressedImage> compressedOut = new SimpleImageQueue<CompressedImage>("CompressedOut", 2);
        CompressedFileWriter writer = null; // FIXMEnew CompressedFileWriter(number++,"Writer", compressedOut, "out-", ".tif", null);

        // Next the compressor
        TIFCompressor jpg = new TIFCompressor();
        ImageQueue<UncompressedImage> scaled = new SimpleImageQueue<UncompressedImage>("Scaled", 2);
        Compressor compressor = new Compressor(number++,"Compressor", scaled, compressedOut, jpg, null);

        writer.start();
        compressor.start();

        scaled.put(image);
        scaled.setDone();
       
       // scaled.put(image);
       // scaled.setDone();
        
        compressor.join();
        writer.join();
    }    

}
