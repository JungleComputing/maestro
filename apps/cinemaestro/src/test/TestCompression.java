package test;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import compression.BlockOutput;
import compression.x264;

public class TestCompression implements BlockOutput {

    private int width = 1280;
    private int height = 720;

    private int repeat = 10;
    private int repeatFrame = 10;
    
    private final int bitRate = 1024*1024;
    private final int frames = 1000;
    
    private int frame;
    
    private final x264 compressor;
    
    private static class Frame { 
        byte [] Y;
        byte [] U;
        byte [] V;
        
        Frame(int pixels, int valY, int valU, int valV) { 
            Y = new byte[pixels];
            U = new byte[pixels/4];
            V = new byte[pixels/4];
           
            Arrays.fill(Y, (byte) valY);
            Arrays.fill(U, (byte) valU);
            Arrays.fill(V, (byte) valV);
        }
    }
    
    private Frame [] data;
    
    private OutputStream out; 

    private byte [][] output;
    private int nextOutput;
    
    private byte [] tmp;
    private int tmpLen;
    
    private TestCompression() throws Exception { 
        compressor = new x264(width, height, frames, bitRate, this);  
    
        data = new Frame[4];
        
        for (int i=0;i<data.length;i++) { 
            data[i] = new Frame(width*height, 255/(i+1), 127, 127);
        }
    
        out = new BufferedOutputStream(new FileOutputStream("eep.h264"));
    
        output = new byte[data.length*repeat*repeatFrame][];
    
        tmp = new byte[1024*1024];
    }
   
    public void store(byte [] buffer, int len) {
        
        System.out.println("Storing " + len + " bytes");
        
        System.arraycopy(buffer, 0, tmp, tmpLen, len);
        tmpLen += len;
    }
    
    public void blockDone() {
        
        System.out.println("Block done " + tmpLen + " bytes");
        
        output[nextOutput] = new byte[tmpLen];
        System.arraycopy(tmp, 0, output[nextOutput], 0, tmpLen);
        
        nextOutput++;
        tmpLen = 0;
    }
    
    
    public byte[] storeBlock(byte[] buffer, int len, boolean needNew, boolean endOfBlock) {
        
        if (buffer != null) { 
            store(buffer, len);
        } 

        if (endOfBlock) { 
            blockDone();
        }
        
        if (!needNew) { 
            return null;
        }
        
        return new byte[1024];
    }
   
    public void writeData() throws IOException { 
        
        out.write(output[0]);
        out.write(output[0]);
        out.write(output[0]);
        out.write(output[0]);
        
        for (int i=1;i<nextOutput;i+=4) { 
            out.write(output[i]);
        }
        
        for (int i=2;i<nextOutput;i+=4) { 
            out.write(output[i]);
        }
        
        for (int i=3;i<nextOutput;i+=4) { 
            out.write(output[i]);
        }

        for (int i=0;i<nextOutput;i+=4) { 
            out.write(output[i]);
        }

        for (int i=0;i<nextOutput;i++) { 
            out.write(output[i]);
        }
        
        out.close();
    }
    
    public void writeFrame(Frame f, int repeat) throws Exception { 
        
        for (int i=0;i<repeat;i++) { 
            System.out.println("Writing frame " + frame++ + " (" + i + ")");
            compressor.add(f.Y, f.U, f.V);
        }
        
        compressor.flush(); 
    }
   
    public void start() throws Exception { 
        
        for (int r=0;r<repeat;r++) { 
            for (int f=0;f<data.length;f++) { 
                writeFrame(data[f], repeatFrame);
            }   
        }
        
        writeData();
    }
    
    public static void main(String [] args) { 

        try {
            new TestCompression().start();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }

    
}
