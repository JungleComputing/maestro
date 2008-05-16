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

import processors.CompressedFileReader;
import processors.Compressor;
import processors.Decompressor;
import processors.ProcessorThread;
import processors.Scaler;
import processors.StreamCombiner;
import util.FileFinder;
import util.FileInfo;
import util.FilesetDescription;

public class MultiFileAndDecompressor {
    
    private static int repeat = 0;        
    
    private static int framesPerBlock = 30;
    private static int width = 1280;
    private static int height = 720;
    private static int bitrate = 1024*1024;
    
    private static int readers = 1;
    private static int decompressor = 1;
    
    private static String directory;
    private static String prefix;
    private static String postfix;
    
    private static File outputFile;
    
    private String getProcessed(ProcessorThread [] p) { 
        
        String decom = "(" + p[0].getProcessedImage();
        
        for (int i=1;i<decompressor;i++) { 
            decom = decom + " / " + p[i].getProcessedImage();
        }
        
        return decom + ")";
    }
    
    @SuppressWarnings("unchecked")
    private void start() {

        FileOutputStream out = null;
        
        try { 
            System.out.println("Multi-FileReader/Decompressor Compression using:");
            System.out.println("   readers       : " + readers);
            System.out.println("   decompressors : " + decompressor);
            System.out.println("   scalers       : " + 1);
            System.out.println("   compressors   : " + 1);
            System.out.println("   combiners     : " + 1);
            System.out.println("   input dir     : " + directory);
            System.out.println("   output file   : " + outputFile.getName());
            
            out = new FileOutputStream(outputFile);

            // Create all of the necessary components back to front 
            
            int number = 0;
            
            // First the output combiner
            ImageQueue<CompressedImage> compressedOut = new SimpleImageQueue<CompressedImage>("CompressedOut", 2);
            StreamCombiner combiner = new StreamCombiner(number++,"StreamCombiner", compressedOut, out, null);
            
            // Next the compressor
            H264Compressor h264 = new H264Compressor(width, height, framesPerBlock, bitrate);
            ImageQueue<UncompressedImage> scaled = new SimpleImageQueue<UncompressedImage>("Scaled", 2);
            Compressor compressor = new Compressor(number++,"Compressor", scaled, compressedOut, h264, null);
            
            // Next the scaler 
            RoundRobinInputQueue<UncompressedImage> decompressed = 
                new RoundRobinInputQueue<UncompressedImage>("Decompressed", 2, decompressor*readers);
            Scaler scaler = new Scaler(number++,"Scaler", decompressed, scaled, width, height, 0, null);
            
            // Next the decompressors and readers.
            // We create 'reader' readers here, with 'decompress' decompressors
            // each. We therefore need 'reader' RoundRobinOutputQueue' to 
            // connect the readers to the decompressors. 
            //
            // Note: this becomes a bit messy, but it would be trivial to draw
            // if a flow-graph type GUI....
            
            RoundRobinOutputQueue<CompressedImage> [] compressedIn = new RoundRobinOutputQueue[readers];
            
            for (int i=0;i<readers;i++) { 
                compressedIn[i] = new RoundRobinOutputQueue<CompressedImage>("CompressedIn", 2, decompressor);
            }
            
            Decompressor [] decompressors = new Decompressor[decompressor*readers];
           
            for (int j=0;j<readers;j++) { 
                for (int i=0;i<decompressor;i++) { 
                    ImageDecompressor [] tif = new ImageDecompressor [] { new TIFImageDecompressor() };
                    decompressors[j*decompressor + i] = new Decompressor(number++,"Decompressor." + j + "." + i, 
                            compressedIn[j].getQueue(i), decompressed.getQueue(j*decompressor + i), tif, null);    
                }
            }
            
            CompressedFileReader [] r = new CompressedFileReader[readers];
            
            for (int i=0;i<readers;i++) { 
                
                FilesetDescription desc = new FilesetDescription(false, directory, prefix, postfix, 4);
                
                FileInfo [] files = FileFinder.find(desc, i, readers); 
                r[i] = new CompressedFileReader(number++,"FileReader." + i, compressedIn[i], repeat, files, null);
            }
            
            // Start the lot
            System.out.println("Starting...");
            
            for (CompressedFileReader rd : r) { 
                rd.start();
            }
            
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
                
                System.out.println(sec + ": " + 
                        getProcessed(r) + " -> " + 
                        getProcessed(decompressors)  + " -> " +
                        scaler.getProcessedImage()  + " -> " +
                        compressor.getProcessedImage()  + " -> " +
                        combiner.getProcessedImage());
                
                try { 
                    Thread.sleep(10*1000);
                } catch (Exception e) {
                    // ignore
                }
         
                // If the last is done, there all done ;-)
                done = combiner.getDone();
            }
            
            for (CompressedFileReader rd : r) { 
                rd.join();
            }
            
            for (Decompressor d : decompressors) { 
                d.join();
            }
            
            scaler.join();
            compressor.join();
            combiner.join();
            
            for (CompressedFileReader rd : r) { 
                rd.printStatistics();
            }
                
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
            } else if (args[i].equals("-decompressorsPerFile")) {
                decompressor = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-readers")) {
                readers = Integer.parseInt(args[++i]);
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
       // if (!(directory !=  null && directory.exists() && directory.canRead())) { 
       //     System.out.println("Directory " + directory + " not found!");
       //     System.exit(1);
       // }

        if (prefix == null || postfix == null) { 
            System.out.println("Filename prefix/postfix not set!");
            System.exit(1);
        }
        
        if (outputFile == null) { 
            System.out.println("Outputfile not set!");
            System.exit(1);
        }
        
        new MultiFileAndDecompressor().start();        
    }
}
