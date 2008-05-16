package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.StringTokenizer;

public class ProcessOutput {

    private final LinkedList<File> input;
    
    private long [] data;
    private double [] messages;
    
    private int size = 1000;
    private int maxIndex = 0;
   
    private double prevTime = 0.0;
    
    private static double rate = 1000.0*1000.0*1000.0;
    
    public ProcessOutput(LinkedList<File> input) { 
        this.input= input;
        data = new long[size];
        messages = new double[size];
    }
 
    private void addData(double time, long m, long bytes) {
        
        if (m == 0) { 
            return;
        }
        
        double dt = time - prevTime;
    
        double tmpM = 0.0;
        long tmpBytes = 0;
        
        if (dt < 1.0) { 
            
            int index = (int) time;
            
            data[index] += bytes;
            messages[index] += m;
        
            if (index > maxIndex) { 
                maxIndex = index;
            }
        
        } else { 
            
            int index = (int) time;
      
            if (index > maxIndex) { 
                maxIndex = index;
            }
            
            int prevIndex = (int) prevTime;
        
            double part = 1 - (prevTime - prevIndex);

            data[prevIndex] += (long) ((bytes * part) / dt);
            messages[prevIndex] += (m * part) / dt;
            
            tmpM += m * (part  / dt);
            tmpBytes +=  (long) ((bytes * part) / dt);
            
            for (int i=prevIndex+1;i<index;i++) { 
                data[prevIndex] += (long) (bytes / dt);
                messages[prevIndex] += m / dt;
                tmpM += m / dt;
                tmpBytes += (long) (bytes / dt);
            }
                
            part = time - index;

            data[index] += (long) ((bytes * part) / dt);
            messages[index] += (m * part) / dt;
            
            tmpM += (m * part) / dt;
            tmpBytes +=  (long) ((bytes * part) / dt);
        }
        
        prevTime= time;
        
  //      System.out.println((index++) + " " + time + " " + dt + " " 
   //             + m + " (" + tmpM  + ") " 
   //             + bytes + " (" + tmpBytes  + ") ");
      
    }
    
    private void processLine(String line) { 
   
        StringTokenizer t = new StringTokenizer(line);
        
        double time = Double.parseDouble(t.nextToken());
        long messages = Long.parseLong(t.nextToken());
        long bytes = Long.parseLong(t.nextToken());

        addData(time, messages, bytes);
    }
    
    private void processFile(File f) throws IOException { 
        
        BufferedReader r = new BufferedReader(new FileReader(f));
        
        String line = r.readLine();
        
        while (line != null) { 
    
            if (line.startsWith("[*]")) { 
                // skip comment
            } else { 
                processLine(line);
            }
            
            line = r.readLine();
        }
        
        
        
    }
   
    public void start() throws IOException { 
        
        for (File f : input) { 
            processFile(f);
        }
        
        for (int i=0;i<maxIndex;i++) { 
            
            double gbit = (8.0*data[i]) / rate;
            
            System.out.printf("%d %.2f %.2f\n", i, messages[i], gbit);
        }
    }
    
    public static void main(String [] args) throws IOException { 
        
        LinkedList<File> input = new LinkedList<File>();
        
        for (int i=0;i<args.length;i++) {
            
            if (args[i].equals("-mbit")) { 
                rate = 1000.0*1000.0;
            } else if (args[i].equals("-kbit")) { 
                rate = 1000.0;
            } else if (args[i].equals("-gbit")) { 
                rate = 1000.0*1000.0*1000.0;
            } else { 
                File tmp = new File(args[i]);
                input.addLast(tmp);
            }
        }
        
        new ProcessOutput(input).start();
        
    }
    
    
}
