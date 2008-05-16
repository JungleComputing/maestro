package test;

import image.CompressedImage;
import image.ImageQueue;
import image.RGB48Image;
import image.UncompressedImage;
import image.compression.JPEGCompressor;
import image.queues.SimpleImageQueue;
import processors.CompressedFileWriter;
import processors.Compressor;
import processors.Scaler;

public class TestJPG {

    public static void main(String [] args) throws Exception {

        int width = 3840;
        int height = 2160;

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
        CompressedFileWriter writer = null; // FIXMEnew CompressedFileWriter(number++,"Writer", compressedOut, "out-", ".jpg", null);

        // Next the compressor
        JPEGCompressor jpg = new JPEGCompressor();
        ImageQueue<UncompressedImage> scaled = new SimpleImageQueue<UncompressedImage>("Scaled", 2);
        Compressor compressor = new Compressor(number++,"Compressor", scaled, compressedOut, jpg, null);

        // Next the scaler 
        ImageQueue<UncompressedImage> decompressed = new SimpleImageQueue<UncompressedImage>("Decompressed", 2);
        Scaler scaler = new Scaler(number++,"Scaler", decompressed, scaled, 1280, 720, 0, null);
        
        writer.start();
        scaler.start();
        compressor.start();

                
        /*
        byte [] data = new byte[1024 * 512 * 3];
        
        int edge = 256;
        
        int height = 512;
        int width = 1024;
        
        for (int h=0;h<edge;h++) { 
            for (int w=0;w<1024;w++) { 
                data[3*h*width +3*w] = (byte) 255;;
                data[3*h*width +3*w+1] = 0; // (short) ((Short.MAX_VALUE * 2) & 0xffff);
                data[3*h*width +3*w+2] = 0; // (short) ((Short.MAX_VALUE * 2) & 0xffff);
            }
         }
     
        
        for (int h=edge;h<512;h++) { 
            for (int w=0;w<1024;w++) { 
                data[3*h*width +3*w] = 0; // (short) ((Short.MAX_VALUE * 2) & 0xffff);
                data[3*h*width +3*w+1] = (byte)255; // (short) ((Short.MAX_VALUE * 2) & 0xffff);
                data[3*h*width +3*w+2] = 0; //(short) ((Short.MAX_VALUE * 2) & 0xffff);
            }
         }
        */
        
      //  RGB24Image image = new RGB24Image(0, 1024, 512, null, data);
        
        decompressed.put(image);
        decompressed.setDone();
       
       // scaled.put(image);
       // scaled.setDone();
        
        compressor.join();
        writer.join();
        scaler.join();
        
    }    

}
