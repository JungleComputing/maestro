package processors;

import image.CompressedImage;
import image.ImageQueue;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.HashMap;

import util.FilesetDescription;
import util.Options;
import util.config.ComponentDescription;

public class CompressedFileWriter extends ImageConsumer<CompressedImage> {
    
    private final File dir;
    private final FilesetDescription out;
    
    public CompressedFileWriter(int componentNumber, String name, 
            ImageQueue<CompressedImage> in, FilesetDescription fs,
            StatisticsCallback publisher) {
        
        super(componentNumber, "CompressedFileWriter", name, in, publisher);        
   
        this.out = fs;
        this.dir = new File(out.getDir());
    }
    
    private String getName(long number) { 
        
        long tmp = 10;
        
        String name = out.getPrefix();
        
        for (int i=1;i<out.getPositions();i++) { 
            if (number < tmp) {
                name += "0";
            }
            
            tmp = tmp * 10;
        }
        
        return name + number + out.getPostfix();
    }
    
    
    public void process() { 
  
        CompressedImage i = in.get();

        long next = i.number;
        
        while (i != null) {

            logger.info("Got image " + i.number + " expected " + next);

            // Santity check 
       //     if (next != i.number) { 
        //        logger.warn("Got wrong image " + i.number + " expected " + next);
        //    }
            
            String name = getName(i.number);
            
            File file = new File(dir, name);        
            
            long t1 = System.currentTimeMillis();
            
            try {
                RandomAccessFile raf = new RandomAccessFile(file, "rw");
                raf.write((byte []) i.getData());
                raf.close();
            } catch (Exception e) { 
                System.out.println("[*] ERROR (" + name 
                        + "): failed to write " + name);
                e.printStackTrace();
            }

            long t2 = System.currentTimeMillis();
            
            long len = i.getSize();
            
            processedImage(i.number, t2-t1, len, len, in.getMayBlock());
            
            next++;
            
            i = in.get();
        }
    }
    
    public static Class getInputQueueType() {
        return CompressedImage.class;
    }
    
    public static Class getOutputQueueType() {
        return null;
    }
    
    public static CompressedFileWriter create(ComponentDescription c,
            ImageQueue<CompressedImage> in, ImageQueue out, 
             StatisticsCallback publisher) throws Exception {
   
        HashMap<String, String> options = c.getOptions();
        
        String tmp = Options.getStringOption(options, "output");
        
        if (!tmp.startsWith("fileset://")) { 
            throw new Exception("Fileset required!");
        }

        FilesetDescription fs = c.getFileSet(tmp.substring(10));
        
        if (fs == null) { 
            throw new Exception("Fileset required!");
        }
        
        File directory = new File(fs.getDir());
        
        if (!directory.exists() || !directory.canWrite()) { 
            throw new Exception("Directory " + directory + " not accessible!");
        }
        
        return new CompressedFileWriter(c.getNumber(), c.getName(), in, fs, publisher);
    }
    
}

