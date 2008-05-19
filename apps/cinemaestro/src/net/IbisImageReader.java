package net;

import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import image.Image;
import image.ImageQueue;

public class IbisImageReader extends Thread {
   
    private final ImageQueue<Image> out;
    private final ReceivePort in; 
    private boolean closed = false;

    private long readTime; 
    private long putTime; 
    private long bytes;
    
    public IbisImageReader(ReceivePort in, ImageQueue<Image> out) {
        this.out = out;
        this.in = in;
    }

    public void close() {
        
        if (closed) { 
            return;
        }
        
        try {
            in.close();
        } catch (Exception e) {
            System.out.println("[*] ERROR(IbisImageReader): failed to close!");
            e.printStackTrace();
        }
            
        closed = true;
    }

    public Image readImage() {
        
        if (closed) { 
            return null;
        }
        
        try {
            ReadMessage r = in.receive();
    
            long start = System.currentTimeMillis();
            
            boolean more = r.readBoolean();
            
            if (!more) { 
                r.finish();
                return null;
            }
            
            Image i = (Image) r.readObject();
            
            bytes += r.finish();
            
            readTime += System.currentTimeMillis() - start;
            
            return i;
            
        } catch (Exception e) {
            System.out.println("[*] ERROR(IbisImageReader): failed to receive message!");
            e.printStackTrace();
            close();
            return null;
        }
    }
    
    @Override
    public void run() {
        
        boolean cont = true;
        
        long start = System.currentTimeMillis();
        
        while (cont) { 
        
            Image image = readImage();
            
            if (image != null) { 
           
                long tmp = System.currentTimeMillis();
                
                out.put(image);
                
                putTime += System.currentTimeMillis() - tmp;
                
            } else { 
                cont = false;
            }
        }
      
        out.setDone();
   
        long end = System.currentTimeMillis();
        
        double tpS = ((1000.0*bytes) / (1024*1024)) / readTime;
        double tpA = ((1000.0*bytes) / (1024*1024)) / (end-start);
        
        System.out.println("[*] Ibis READER took " + (end-start) + " read " 
                + readTime + " put " + putTime + " bytes " + bytes 
                + " " + tpS + " " + tpA + " (connected to: " 
                + in.connectedTo()[0].ibisIdentifier() + ")");
        
        close();
    }
}
