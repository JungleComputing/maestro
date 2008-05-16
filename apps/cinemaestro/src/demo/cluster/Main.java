package demo.cluster;

import java.io.File;

import util.config.Config;
import util.config.Config;

public class Main {

    public static void main(String [] args) { 
        try { 
            if (args.length == 2 && args[0].equals("-master")) { 
                Config c = new Config(new File(args[1]));
                c.parse();
                //c.print();
                
                // Start a master
                Master m = new Master(c);
                m.start();
            } else { 
                // Start a worker
                Worker w = new Worker();
                w.start();
            }
                    
        } catch (Exception e) {
            System.out.println("Run failed!");
            e.printStackTrace();
        }
    }
}
