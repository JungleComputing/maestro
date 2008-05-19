package processors;

import image.Image;
import image.ImageQueue;
import image.RGB24Image;
import image.RGB48Image;
import image.UncompressedImage;

import java.io.EOFException;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.LinkedList;

import util.FileFinder;
import util.FileInfo;
import util.FilesetDescription;
import util.Options;
import util.config.ComponentDescription;

public class UncompressedFileReader extends ImageProducer<UncompressedImage> {
    
    private LinkedList<FileInfo> files = new LinkedList<FileInfo>();
    
    private int repeat;
    private long frameGap = 0;
    private long startDelay = 0;
    
    public UncompressedFileReader(int componentNumber, String name, 
            ImageQueue<UncompressedImage> out, int repeat, int fps, 
            boolean delayStart, int rank, int size, 
            FileInfo [] files, StatisticsCallback publisher) {
        
        super(componentNumber, "UncompressedFileReader", name, out, publisher);
        
        this.repeat = repeat;
              
        if (fps > 0) { 
            frameGap = (1000*size) / fps; 
        }   
        
        if (delayStart && fps > 0) { 
            startDelay = (rank * 1000) / fps;
        }
        
        if (files != null) { 
            for (FileInfo f : files) { 
                this.files.add(f);
            }
        }
        
        setTotalImages(this.files.size());
    }
   
    private ByteBuffer buffer = null;
    
    private UncompressedImage readImage(FileInfo f) throws Exception {
        
        // DataInputStream in = new DataInputStream(new BufferedInputStream(
        
        FileInputStream fs = new FileInputStream(f.file);
        
        try { 
            
            FileChannel channel = fs.getChannel();
            
            int len = (int) f.file.length();
            
            if (buffer == null || buffer.capacity() < len) { 
                System.out.println("[*] Allocating buffer!");
                buffer = ByteBuffer.allocateDirect(len);
            }
            
            long size = 0;
            
            while (size != len) { 
                long tmp = channel.read(buffer);
                
                if (tmp == -1) { 
                    throw new EOFException();
                }
                
                size += tmp;
            }
            
            buffer.flip();
            
            int bpp = buffer.getInt();
            int width = buffer.getInt();
            int height = buffer.getInt();
            int pixels = width * height;

            if (bpp == 24) { 
                byte [] data = new byte[pixels * 3];
                buffer.put(data);
                buffer.clear();
                return new RGB24Image(f.number, width, height, f.file, data);

            } else if (bpp == 48) {
                short [] data = new short[pixels * 3];
                (buffer.asShortBuffer()).put(data);
                buffer.clear();
                return new RGB48Image(f.number, width, height, f.file, data);
            } else { 

                buffer.clear();
                throw new Exception("Unsupported image type!");
            }
        } finally { 
            try { 
                fs.close();
            } catch (Exception e) {
                // TODO: handle exception
            }
        }
    }
    
    @Override
    public void process() {

        System.out.println("[*] FILE READER TARGET GAP " + frameGap);
      
        long images = 0;
        
        long last = 0;
        int r = 0;
        
        do { 
            
            for (FileInfo f : files) { 
                
                try { 
                    long t1 = System.currentTimeMillis();
                
                    UncompressedImage c = readImage(f);
                
                    long t2 = System.currentTimeMillis();
                
                    processedImage(c.number, t2-t1, c.getSize(), c.getSize(), 
                            out.putMayBlock());
                    
                    if (startDelay > 0) { 
                        Thread.sleep(startDelay);
                        startDelay = 0;
                    }
                    
                    if (frameGap > 0) { 
                        if (last > 0) { 
                            // Lets produce the frames 3 ms. early, just to be sure
                            long sleep = frameGap - (System.currentTimeMillis() - last) - 3;
                            
                            if (sleep > 0) { 
                                Thread.sleep(sleep);
                            } else { 
                                System.out.println("[*] FILE READER MISSED DEADLINE! " + sleep);
                            }
                        }

                        last = System.currentTimeMillis();
                    } 
                    
                    
                    out.put(c);
                    
                    images++;
                    
                } catch (Exception e) { 
                    logger.warn("Could not load file " + f, e);
                }
            }
            
        } while (++r < repeat);
        
        out.setDone();
    }

    public static Class<Image> getInputQueueType() { 
        return null;
    }
    
    public static Class<UncompressedImage> getOutputQueueType() {
        return UncompressedImage.class;
    }    
    
    public static UncompressedFileReader create(ComponentDescription c, 
            ImageQueue in, ImageQueue<UncompressedImage> out,
            StatisticsCallback publisher) throws Exception {
   
        HashMap<String, String> options = c.getOptions();
        
        int fps = Options.getIntOption(options, "fps", false, 0);
        int repeat = Options.getIntOption(options, "repeat", false, 0);
      
        boolean delayStart = Options.getBooleanOption(options, "delay", false, false);
        
        String tmp = Options.getStringOption(options, "input", true, null);

        if (!tmp.startsWith("fileset://")) { 
            throw new Exception("Fileset required!");
        }

        FilesetDescription fs = c.getFileSet(tmp.substring(10));
        
        if (fs == null) { 
            throw new Exception("Fileset required!");
        }
        
        FileInfo [] f = FileFinder.find(fs, c.getRank(), c.getSize());
        
        return new UncompressedFileReader(c.getNumber(), c.getName(), out, 
                repeat, fps, delayStart, c.getRank(), c.getSize(), f, publisher);
    }
    
}
