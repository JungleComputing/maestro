package compression;

public class x264 {
    
    private static final String LIB = "libCompression.so";
    private static boolean libraryInitialized = false;
    
    private native int x264_init(int w, int h, int frames, int bitrate);
    private native int x264_add_frame(byte [] Y, byte [] U, byte [] V);    
//    private native int x264_flush();    
    private native int x264_done();
    
    private final int width;
    private final int height;    
    private final int frames;
    private final int bitrate;
    
    private final BlockOutput out;
    
    private synchronized static void initialiseLibrary() { 

        if (libraryInitialized) { 
            return;
        }
        
        boolean done = false;
        
        if (!done) { 
            String lib = System.getProperty("user.dir") + "/" + LIB;

            try { 
                System.load(lib);
                done = true;
            } catch (Throwable e) { 
                System.err.println("Failed to load " + lib);
                e.printStackTrace(System.err);
            }
        }

        if (!done) { 
            String lib = System.getProperty("user.home") + "/" + LIB;

            try { 
                System.load(lib);
                done = true;
            } catch (Throwable e) { 
                System.err.println("Failed to load " + lib);
                e.printStackTrace(System.err);
            }
        }

        if (!done) { 
            try { 
                System.loadLibrary(LIB);
                done = true;
            } catch (Throwable e) { 
                System.err.println("Failed to load native library");
                e.printStackTrace(System.err);
            }
        }
        
        if (!done) { 
            throw new Error("Failed to load library!!!");
        } 
 
        libraryInitialized = true;
    }
    
    public x264(int w, int h, int frames, int bitrate, BlockOutput out) throws Exception {
        
        System.out.println("Initializing x264 " + w + "x" + h + " (" 
                + frames + ")");

        initialiseLibrary();
        
        this.width = w;
        this.height = h;
        this.frames = frames;
        this.out = out;
        this.bitrate = bitrate;
        
        if (x264_init(w, h, frames, bitrate) != 0) { 
            throw new Exception("Failed to init x264 library");
        }
    }
    
    protected byte [] exchangeBuffer(byte [] buffer, int len, boolean needNew, boolean endOfBlock) { 
        return out.storeBlock(buffer, len, needNew, endOfBlock);
    }
    
    public void add(byte [] Y, byte [] U, byte [] V) throws Exception {
        
        if (x264_add_frame(Y, U, V) != 0) { 
            throw new Exception("Failed to add frame!");
        }    
    }
    
    public void done() throws Exception { 
        
        if (x264_done() != 0) { 
            throw new Exception("Failed close x264");            
        }
    }
    
    public void flush() throws Exception { 
        
        //if (x264_flush() != 0) { 
        //    throw new Exception("Failed flush x264");            
        //}

       // System.out.println("Flushing x264");
        
        if (x264_done() != 0) { 
            throw new Exception("Failed close x264 (in flush)");            
        }

     //   System.out.println("Done OK");
                
        if (x264_init(width, height, frames, bitrate) != 0) { 
            throw new Exception("Failed to init x264 library (in flush)");
        }

     //   System.out.println("Init OK");

    }
    
    /*
    public static void main(String [] args) throws Exception { 

        FileOutputStream out = new FileOutputStream("test");
        FileOutputStream yuv = new FileOutputStream("test.yuv");
         
        x264 c = new x264(1920, 1080, 100, 1024, out);
        
        c.test(yuv);
        c.done();
        
        yuv.close();
        out.close();
    }*/
    
    
}