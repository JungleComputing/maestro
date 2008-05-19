package net;

import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;
import image.Image;
import image.ImageQueue;
import java.io.IOException;

public class IbisImageWriter extends Thread {
    
    private final ImageQueue in;
    private final SendPort out;
    private boolean closed = false;
    
    private long totalBytes;
    private long totalSendTime;
    private long totalGetTime;
    
    private long bytes;
    private long count;
    
    public IbisImageWriter(ImageQueue in, SendPort out) { 
        this.in = in;
        this.out = out;
    }

    public void close() {
        
        if (closed) { 
            return;
        }
    
        try { 
            WriteMessage m = out.newMessage();
            m.writeBoolean(false);
            m.finish();
            
            out.close();
        } catch (Exception e) {
            System.out.println("[*] ERROR (IbisImageWriter): failed to close port!");
            e.printStackTrace();
        }
        closed = true;
    }

    private boolean init() {        
        ReceivePortIdentifier [] tmp = out.connectedTo();
        return (tmp.length > 0);
    }
    
    public synchronized void addStats(long [] target) { 
        target[0] += count; 
        target[1] += bytes; 
        
        bytes = 0;
        count = 0;
    }
    
    public boolean writeImage(Image image) {
        
        if (closed) { 
            return false;
        }
             
        try {
            long start = System.currentTimeMillis();
            
            WriteMessage m = out.newMessage();
            m.writeBoolean(true);
            m.writeObject(image);
            long tmp = m.finish();
        
            long end = System.currentTimeMillis();

            totalBytes += tmp;
            totalSendTime += (end-start);            

            synchronized (this) { 
                count++;
                bytes += tmp;
            }
            return true;
        } catch (IOException e) {
            System.out.println("[*] ERROR (IbisImageWriter): failed to send image!");
            e.printStackTrace();
            close();
            return false;
        }
        
        
    }
    
    @Override
    public void run() {
        
        boolean cont = init();
        
        long start = System.currentTimeMillis();
        
        while (cont) { 
            
            long tmp = System.currentTimeMillis();
            
            Image image = in.get();
            
            totalGetTime += System.currentTimeMillis()- tmp;
            
            if (image != null) { 
                cont = writeImage(image);
            } else { 
                cont = false;
            }
        }
        
        long end = System.currentTimeMillis(); 
      
        double tpS = ((1000.0*totalBytes) / (1024*1024)) / totalSendTime;
        double tpA = ((1000.0*totalBytes) / (1024*1024)) / (end-start);
        
        System.out.println("[*] Ibis WRITER took " + (end-start) + " send " 
                + totalSendTime + " get " + totalGetTime + " bytes " + totalBytes 
                + " " + tpS + " " + tpA);
        
        close();
    }

}
