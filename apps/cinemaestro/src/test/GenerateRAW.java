package test;

import image.ImageQueue;
import image.UncompressedImage;
import image.queues.SimpleImageQueue;

import java.io.File;

import processors.Generator;
import processors.UncompressedFileWriter;
import util.FilesetDescription;

public class GenerateRAW {
         
    private final FilesetDescription out;
    
    private final int width;
    private final int height;
    private final int frames;
    private final int bits;
    private final int rank;
    private final int size;
    
    private GenerateRAW(FilesetDescription out, int w, int h, int frames, 
            int bits, int rank, int size) { 
        
        this.out = out;     
        this.width = w;
        this.height= h;
        this.frames = frames;
        this.bits = bits;
        this.rank = rank;
        this.size = size;
    }
    
    private void start() {
        
        try { 
            // Create all of the necessary components back to front 
            int number = 0;
            
            // First the output combiner
            ImageQueue<UncompressedImage> outq = new SimpleImageQueue<UncompressedImage>("out", 2);
            UncompressedFileWriter writer = new UncompressedFileWriter(number++, "Writer", outq, out, null);
            Generator generator = new Generator(number++, "Generator", outq, width, height, frames, bits, rank, size, 0, null);
            
            // Start the lot
            System.out.println("Starting...");
            
            writer.start();
            generator.start();
             
            // Wait for everyone to finish
            generator.join();
            writer.join();
            
            generator.printStatistics();
            writer.printStatistics();
            
            // Done!
            System.out.println("Done!");
            
        } catch (Exception e) {

            System.out.println("EEP! " + e);
            e.printStackTrace();
            
        } 
    }    
    
    private static FilesetDescription parseFileSet(String file) throws Exception { 
        
        if (file == null || file.length() == 0) { 
            throw new Exception("Fileset line empty!");
        }

        System.out.println("Parsing fileset: " + file);
        
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
        String output = null;
        
        int width = 0;
        int height = 0;
        int frames = 0;
        int bits = 0;
        
        for (int i=0;i<args.length;i++) {             
            
            if (args[i].equals("-output")) {
                output = args[++i];
            } else if (args[i].equals("-width")) {
                width = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-height")) {
                height = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-frames")) {
                frames = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-bits")) {
                bits = Integer.parseInt(args[++i]);
            } else {
                rank = Integer.parseInt(args[i++]);
                size = Integer.parseInt(args[i++]);
            }
        }

        if (output == null) { 
            System.out.println("EEP!");
            System.exit(1);
        }
        
        FilesetDescription fsOut = parseFileSet(output);
        
        new GenerateRAW(fsOut, width, height, frames, bits, rank, size).start();        
    }
}