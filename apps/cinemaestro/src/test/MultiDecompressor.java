package test;

import image.CompressedImage;
import image.ImageDecompressor;
import image.ImageQueue;
import image.UncompressedImage;
import image.compression.H264Compressor;
import image.decompression.TIFImageDecompressor;
import image.queues.RoundRobinInputQueue;
import image.queues.RoundRobinOutputQueue;
import image.queues.SimpleImageQueue;

import java.io.File;
import java.io.FileOutputStream;

import processors.StreamCombiner;
import processors.CompressedFileReader;
import processors.Compressor;
import processors.Decompressor;
import processors.Scaler;

import util.FileFinder;
import util.FileInfo;
import util.FilesetDescription;

public class MultiDecompressor {
    
    private static int repeat = 0;        
    
    private static int framesPerBlock = 30;
    private static int width = 1280;
    private static int height = 720;
    private static int bitrate = 1024*1024;
    private static int decompressor = 1;
    
    private static String directory;
    private static String prefix;
    private static String postfix;
    
    private static File outputFile;
    
    private void start() {

        FileOutputStream out = null;
        
        try { 
            System.out.println("Multi-decompressor Compression using:");
            System.out.println("   readers       : " + 1);
            System.out.println("   decompressors : " + decompressor);
            System.out.println("   scalers       : " + 1);
            System.out.println("   compressors   : " + 1);
            System.out.println("   combiners     : " + 1);
            System.out.println("   input dir     : " + directory);
            System.out.println("   output file   : " + outputFile.getName());
            
            out = new FileOutputStream(outputFile);

            FilesetDescription desc = new FilesetDescription(false, directory, prefix, postfix, 4);
            
            FileInfo [] files = FileFinder.find(desc); 

            // Create all of the necessary components back to front 
            
            int number = 0;
            
            // First the output combiner
            ImageQueue<CompressedImage> compressedOut = new SimpleImageQueue<CompressedImage>("CompressedOut", 2);
            StreamCombiner combiner = new StreamCombiner(number++, "StreamCombiner", compressedOut, out, null);
            
            // Next the compressor
            H264Compressor h264 = new H264Compressor(width, height, framesPerBlock, bitrate);
            ImageQueue<UncompressedImage> scaled = new SimpleImageQueue<UncompressedImage>("Scaled", 2);
            Compressor compressor = new Compressor(number++, "Compressor", scaled, compressedOut, h264, null);
           
            /*
            // Next the scaler 
            ImageQueue<UncompressedImage> decompressed = new SimpleImageQueue<UncompressedImage>("Decompressed", 2);
            Scaler scaler = new Scaler("Scaler", decompressed, scaled, width, height);
            
            // Next the decompressor
            TIFImageDecompressor tif = new TIFImageDecompressor();
            ImageQueue<CompressedImage> compressedIn = new SimpleImageQueue<CompressedImage>("CompressedIn", 2);
            Decompressor decompressor = new Decompressor("Decompressor", compressedIn, decompressed, tif); 
            */
            
            // Next the scaler 
            RoundRobinInputQueue<UncompressedImage> decompressed = 
                new RoundRobinInputQueue<UncompressedImage>("Decompressed", 2, decompressor);
            Scaler scaler = new Scaler(number++, "Scaler", decompressed, scaled, width, height, 0, null);
            
            // Next the decompressor
            
            RoundRobinOutputQueue<CompressedImage> compressedIn = 
                new RoundRobinOutputQueue<CompressedImage>("CompressedIn", 2, decompressor);
            
            Decompressor [] decompressors = new Decompressor[decompressor];
            
            for (int i=0;i<decompressor;i++) { 
                ImageDecompressor [] tif = new ImageDecompressor [] { new TIFImageDecompressor() };
                decompressors[i] = new Decompressor(number++, "Decompressor." + i, 
                        compressedIn.getQueue(i), decompressed.getQueue(i), tif, null);    
            }
            
            // Next the file reader
            CompressedFileReader reader = new CompressedFileReader(number++, "FileReader", compressedIn, repeat, files, null);
            
            // Start the lot
            System.out.println("Starting...");
            
            reader.start();
            
            for (Decompressor d : decompressors) { 
                d.start();
            }
            
            scaler.start();
            compressor.start();
            combiner.start();
            
            // Wait for everyone to finish
            boolean done = false;
            
            long t1 = System.currentTimeMillis();
            
            while (!done) { 
            
                long t2 = System.currentTimeMillis();
                
                int sec = (int) ((t2 - t1) / 1000);
                
                String decom = "(" + decompressors[0].getProcessedImage();
                
                for (int i=1;i<decompressor;i++) { 
                    decom = decom + " / " + decompressors[i].getProcessedImage();
                }
                
                decom = decom + ")";
                
                System.out.println(sec + ": " + reader.getProcessedImage() + " -> " + 
                        decom  + " -> " +
                        scaler.getProcessedImage()  + " -> " +
                        compressor.getProcessedImage()  + " -> " +
                        combiner.getProcessedImage());
                
                try { 
                    Thread.sleep(10*1000);
                } catch (Exception e) {
                    // ignore
                }
            
                done = reader.getDone() && scaler.getDone() && 
                    compressor.getDone() && combiner.getDone();
                
                if (done) { 
                    // Also check the decompressors
                    for (Decompressor d : decompressors) { 
                        done = done && d.getDone();
                    }
                }
            }
            
            reader.join();
            
            for (Decompressor d : decompressors) { 
                d.join();
            }
            
            scaler.join();
            compressor.join();
            combiner.join();
            
            reader.printStatistics();

            for (Decompressor d : decompressors) { 
                d.printStatistics();
            }
           
            scaler.printStatistics();
            compressor.printStatistics();
            combiner.printStatistics();
            
            // Done!
            System.out.println("Done!");
            
        } catch (Exception e) {

            System.out.println("EEP!");
            e.printStackTrace();
            
        } finally { 
            try { 
                out.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }    
    
    public static void main(String [] args) {
                
        for (int i=0;i<args.length;i++) { 
            
            if (args[i].equals("-framesPerBlock")) {
                framesPerBlock = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-decompressors")) {
                decompressor = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-width")) {
                width = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-height")) {
                height = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-bitrate")) {
                bitrate = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-repeat")) {
                repeat = Integer.parseInt(args[++i]);            
            } else if (args[i].equals("-dir")) {
                directory = args[++i];
            } else if (args[i].equals("-out")) {
                outputFile = new File(args[++i]);
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
        if (prefix == null || postfix == null) { 
            System.out.println("Filename prefix/postfix not set!");
            System.exit(1);
        }
        
        if (outputFile == null) { 
            System.out.println("Outputfile not set!");
            System.exit(1);
        }

        new MultiDecompressor().start();        
    }
}