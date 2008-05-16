package test;

import image.CompressedImage;
import image.ImageDecompressor;
import image.ImageQueue;
import image.UncompressedImage;
import image.compression.JPEGCompressor;
import image.decompression.TIFImageDecompressor;
import image.queues.SimpleImageQueue;

import java.io.File;

import processors.CompressedFileReader;
import processors.CompressedFileWriter;
import processors.Compressor;
import processors.Decompressor;
import processors.Scaler;
import util.FileFinder;
import util.FileInfo;
import util.FilesetDescription;

public class SequentialToJPG {
    
    private static int repeat = 0;        
    
    private static int width = 3840;
    private static int height = 2160;
    
    private static String directory;
    private static String prefix;
    private static String postfix;
    
    private void start() {

        try { 
            System.out.println("Main Compression using:");
            System.out.println("   readers     : " + 1);
            System.out.println("   decoders    : " + 1);
            System.out.println("   compressors : " + 1);
            System.out.println("   combiners   : " + 1);
            System.out.println("   input dir   : " + directory);
            
            FilesetDescription desc = new FilesetDescription(false, directory, prefix, postfix, 4);
            
            FileInfo [] files = FileFinder.find(desc); 

            // Create all of the necessary components back to front 
            int number = 0;
            
            // First the output combiner
            ImageQueue<CompressedImage> compressedOut = new SimpleImageQueue<CompressedImage>("CompressedOut", 2);
            CompressedFileWriter writer = null; // FIXME new CompressedFileWriter(number++,"Writer", compressedOut, "out-", ".jpg", null);
            
            // Next the compressor
            JPEGCompressor jpg = new JPEGCompressor();
            ImageQueue<UncompressedImage> scaled = new SimpleImageQueue<UncompressedImage>("Scaled", 2);
            Compressor compressor = new Compressor(number++,"Compressor", scaled, compressedOut, jpg, null);
            
            // Next the scaler 
            ImageQueue<UncompressedImage> decompressed = new SimpleImageQueue<UncompressedImage>("Decompressed", 2);
            Scaler scaler = new Scaler(number++,"Scaler", decompressed, scaled, width, height, 0, null);
            
            // Next the decompressor
            ImageDecompressor [] tif = new ImageDecompressor [] { new TIFImageDecompressor() };
            ImageQueue<CompressedImage> compressedIn = new SimpleImageQueue<CompressedImage>("CompressedIn", 2);
            Decompressor decompressor = new Decompressor(number++,"Decompressor", compressedIn, decompressed, tif, null); 
            
            // Next the file reader
            CompressedFileReader reader = new CompressedFileReader(number++,"FileReader", compressedIn, repeat, files, null);
            
            // Start the lot
            System.out.println("Starting...");
            
            writer.start();
            reader.start();
            decompressor.start();
            scaler.start();
            compressor.start();
            
            // Wait for everyone to finish
            reader.join();
            decompressor.join();
            scaler.join();
            compressor.join();
            writer.join();
            
            // Done!
            System.out.println("Done!");
            
        } catch (Exception e) {

            System.out.println("EEP!");
            e.printStackTrace();
            
        } 
    }    
    
    public static void main(String [] args) {
                
        for (int i=0;i<args.length;i++) { 
            
            if (args[i].equals("-width")) {
                width = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-height")) {
                height = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-repeat")) {
                repeat = Integer.parseInt(args[++i]);            
            } else if (args[i].equals("-dir")) {
                directory = args[++i];
            } else if (args[i].equals("-prefix")) {
                prefix = args[++i];
            } else if (args[i].equals("-postfix")) {
                postfix = args[++i];
            } else { 
                System.out.println("Unknown option " + args[i]);
                System.exit(1);
            }
        }

        // Make sure all parameters are sane        
        //if (!(directory !=  null && directory.exists() && directory.canRead())) { 
         //   System.out.println("Directory " + directory + " not found!");
          //  System.exit(1);
       // }

        if (prefix == null || postfix == null) { 
            System.out.println("Filename prefix/postfix not set!");
            System.exit(1);
        }
        
        new SequentialToJPG().start();        
    }
}
