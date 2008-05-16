package test;

import image.CompressedImage;
import image.ImageDecompressor;
import image.ImageQueue;
import image.UncompressedImage;
import image.compression.JPEGCompressor;
import image.decompression.JPGImageDecompressor;
import image.queues.SimpleImageQueue;

import java.io.File;

import processors.CompressedFileReader;
import processors.CompressedFileWriter;
import processors.Compressor;
import processors.Decompressor;
import util.FileInfo;

public class TestJPGToJPG {

    public static void main(String [] args) throws Exception {

        int number = 0;
        
        // First the output combiner
        ImageQueue<CompressedImage> compressedOut = new SimpleImageQueue<CompressedImage>("CompressedOut", 2);
        CompressedFileWriter writer = null; // FIXMEnew CompressedFileWriter(number++, "Writer", compressedOut, "out-", ".jpg", null);

        // Next the compressor
        JPEGCompressor jpg = new JPEGCompressor();
        ImageQueue<UncompressedImage> scaled = new SimpleImageQueue<UncompressedImage>("Scaled", 2);
        Compressor compressor = new Compressor(number++,"Compressor", scaled, compressedOut, jpg, null);

        // Next the decompressor
        ImageDecompressor [] tif = new ImageDecompressor [] { new JPGImageDecompressor() };
        ImageQueue<CompressedImage> compressedIn = new SimpleImageQueue<CompressedImage>("CompressedIn", 2);
        Decompressor decompressor = new Decompressor(number++,"Decompressor", compressedIn, scaled, tif, null); 
        
        // Next the file reader
        FileInfo [] tmp = new FileInfo [] { new FileInfo(new File(args[0]), 0) };        
        CompressedFileReader reader = new CompressedFileReader(number++,"FileReader", compressedIn, 0, tmp, null);
      
        reader.start();
        writer.start();
        compressor.start();
        decompressor.start();

        compressor.join();
        writer.join();
        decompressor.join();
        reader.join();
    }    

}


