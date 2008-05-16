package test;

import image.CompressedImage;
import image.ImageDecompressor;
import image.ImageQueue;
import image.UncompressedImage;
import image.decompression.TIFImageDecompressor;
import image.queues.SimpleImageQueue;

import java.io.File;
import java.util.StringTokenizer;

import processors.CompressedFileReader;
import processors.Decompressor;
import processors.Scaler;
import processors.ToRGB48;
import processors.UncompressedFileWriter;
import util.FileFinder;
import util.FileInfo;
import util.FilesetDescription;

public class ConvertToRGB48 {
       
    private final FileInfo [] in;
    private final FilesetDescription out;
    
    private final int width;
    private final int height;
    
    private ConvertToRGB48(FileInfo [] in, FilesetDescription out, int w, int h) { 
        this.in = in;
        this.out = out;     
        this.width = w;
        this.height= h;
    }
    
    private void start() {
        
        try { 
            // Create all of the necessary components back to front 
            int number =0;
            
            // First the output combiner
            ImageQueue<UncompressedImage> uncompressedOut = new SimpleImageQueue<UncompressedImage>("out", 2);
            UncompressedFileWriter writer = new UncompressedFileWriter(number++, "writer", uncompressedOut, out, null);
                       
            ImageQueue<UncompressedImage> scaled = new SimpleImageQueue<UncompressedImage>("out", 2);
            ToRGB48 extender = new ToRGB48(number++, "Extender", scaled, uncompressedOut, null);  
            
            ImageQueue<UncompressedImage> decompressed = new SimpleImageQueue<UncompressedImage>("Decompressed", 2);
            Scaler scaler = new Scaler(number++,"Scaler", decompressed, scaled, width, height, 0, null);
            
            // Next the decompressor
            ImageDecompressor [] tif = new ImageDecompressor [] { new TIFImageDecompressor() };
            ImageQueue<CompressedImage> compressedIn = new SimpleImageQueue<CompressedImage>("CompressedIn", 2);
            Decompressor decompressor = new Decompressor(number++, "Decompressor", compressedIn, decompressed, tif, null); 
            
            // Next the file reader
            CompressedFileReader reader = new CompressedFileReader(number++, "FileReader", compressedIn, 0, in, null);
            
            // Start the lot
            System.out.println("Starting...");
            
            extender.start();
            scaler.start();
            decompressor.start();
            writer.start();
            reader.start();
             
            // Wait for everyone to finish
            boolean done = false;
            
            long t1 = System.currentTimeMillis();
            
            while (!done) { 
 
                long t2 = System.currentTimeMillis();
                
                int sec = (int) ((t2 - t1) / 1000);
                
                System.out.println(sec + ": " + reader.getProcessedImage() + " -> " + 
                        decompressor.getProcessedImage()  + " -> " +
                        writer.getProcessedImage());
                
                try { 
                    Thread.sleep(10*1000);
                } catch (Exception e) {
                    // ignore
                }
            
                done = reader.getDone() && decompressor.getDone() && writer.getDone(); 
            }
            
            reader.join();
            scaler.join();
            decompressor.join();
            extender.join();
            writer.join();
            
            reader.printStatistics();
            scaler.printStatistics();
            extender.printStatistics();
            decompressor.printStatistics();
            writer.printStatistics();
            
            // Done!
            System.out.println("Done!");
            
        } catch (Exception e) {

            System.out.println("EEP!");
            e.printStackTrace();
            
        } 
    }    
    
    private static FilesetDescription parseFileSet(String file) throws Exception { 
        
        if (file == null || file.length() == 0) { 
            throw new Exception("Fileset line empty!");
        }

        System.out.println("Parsing fileset: " + file);
        
        StringTokenizer t = new StringTokenizer(file);

        String dir = ".";
        String prefix = null;
        String postfix = null;

        int index = file.lastIndexOf(File.separator);
        
        if (index >= 0) { 
            dir = file.substring(0, index);
            file = file.substring(index+1);
        }

        int first = file.indexOf("%");
        int last  = file.lastIndexOf("%");
        
        if (first == -1) { 
            prefix = file; 
            postfix = "";
        } else { 
            prefix = file.substring(0, first);
            postfix = file.substring(last+1);
        }
        
        int positions = 1 + (last-first); 
        
        System.out.println("Parsed fileset dir=" + dir + " prefix=" + prefix + " postfix="  + postfix + " pos=" + positions);

        return new FilesetDescription(false, dir, prefix, postfix, positions);        
    }
    
    public static void main(String [] args) throws Exception {
         
        int rank = 0;
        int size = 1;
        String input = null;
        String output = null;
        String dir = null;
        
        int width = -1;
        int height = -1;
        
        for (int i=0;i<args.length;i++) {             
            
            
            if (args[i].equals("-input")) {
                input = args[++i];
            } else if (args[i].equals("-output")) {
                output = args[++i];
            } else if (args[i].equals("-width")) {
                width = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-height")) {
                height = Integer.parseInt(args[++i]);
            } else {
                rank = Integer.parseInt(args[i++]);
                size = Integer.parseInt(args[i++]);
            }
        }

        if (input == null || output == null) { 
            System.out.println("EEP!");
            System.exit(1);
        }
        
        FilesetDescription fsIn = parseFileSet(input);
        FilesetDescription fsOut = parseFileSet(output);

        FileInfo [] in = FileFinder.find(fsIn, rank, size);
        
        new ConvertToRGB48(in, fsOut, width, height).start();        
    }
}