package processors;

import image.CompressedImage;
import image.Image;
import image.ImageQueue;
import image.JPGCompressedImage;
import image.TIFCompressedImage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.LinkedList;

import util.FileFinder;
import util.FileInfo;
import util.FilesetDescription;
import util.Options;
import util.config.ComponentDescription;

public class CompressedFileReader extends ImageProducer<CompressedImage> {
    
    private LinkedList<FileInfo> files = new LinkedList<FileInfo>();
    
    private int repeat;
    
    public CompressedFileReader(int componentNumber, String name, ImageQueue<CompressedImage> out, 
            int repeat, FileInfo [] files, StatisticsCallback publisher) {
        
        super(componentNumber, "CompressedFileReader", name, out, publisher);
        
        this.repeat = repeat;
              
        if (files != null) { 
            for (FileInfo f : files) { 
                this.files.add(f);
            }
        }
        
        setTotalImages(this.files.size());
    }
    
    private byte [] readFully(File f) throws IOException { 
    
        RandomAccessFile ra = new RandomAccessFile(f, "r");
        byte [] tmp = new byte[(int)ra.length()];
        ra.readFully(tmp);
        ra.close();
        return tmp;
    }
   
    private CompressedImage readImage(FileInfo f) throws IOException {
        
        byte [] tmp = readFully(f.file);

        if (tmp != null) {
            if (f.file.getName().endsWith(".JPG") || f.file.getName().endsWith(".jpg")) { 
                return new JPGCompressedImage(f.number, f.file.getName(), tmp);                   
            } else {    
                return new TIFCompressedImage(f.number, f.file.getName(), tmp);
            }
        }
        
        throw new IOException("File not found!");
    }
    
    @Override
    public void process() {

        int r = 0;
        
        do { 
            
            for (FileInfo f : files) { 
                
                try { 
                    long t1 = System.currentTimeMillis();
                
                    CompressedImage c = readImage(f);
                
                    long t2 = System.currentTimeMillis();
                
                    processedImage(c.number, t2-t1, c.getSize(), c.getSize(), out.putMayBlock());
               
                    out.put(c);
                    
                } catch (Exception e) { 
                    logger.warn("[*] Could not load file " + f, e);
                }
            }
            
        } while (++r < repeat);
        
        out.setDone();
    }

    public static Class<? extends Image> getInputQueueType() { 
        return null;
    }
    
    public static Class<CompressedImage> getOutputQueueType() {
        return CompressedImage.class;
    }    
    
    public static CompressedFileReader create(ComponentDescription c, 
            ImageQueue in, ImageQueue<CompressedImage> out,
            StatisticsCallback publisher) throws Exception {
   
        HashMap<String, String> options = c.getOptions();
        
        int repeat = Options.getIntOption(options, "repeat", false, 0);
        String tmp = Options.getStringOption(options, "input", true, null);

        if (!tmp.startsWith("fileset://")) { 
            throw new Exception("Fileset required!");
        }

        FilesetDescription fs = c.getFileSet(tmp.substring(10));
        
        if (fs == null) { 
            throw new Exception("Fileset required!");
        }
        
        FileInfo [] f = FileFinder.find(fs, c.getRank(), c.getSize());
        
        return new CompressedFileReader(c.getNumber(), c.getName(), out, repeat, f, publisher);
    }
    
}
