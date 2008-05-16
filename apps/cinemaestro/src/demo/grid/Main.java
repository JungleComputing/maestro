package demo.grid;

import java.io.File;

import util.config.Config;
import util.config.Config;

public class Main {

    public static void main(String [] args) { 
        try { 
            
            boolean master = false;
            File config = null;
            String type = null;
            
            boolean serIbis = true;
            
            for (int i=0;i<args.length;i++) { 
                
                if (args[i].equals("-master")) { 
                    master = true;
                } else if (args[i].equals("-config")) {
                    config = new File(args[++i]);
                } else if (args[i].equals("-type")) { 
                    type = args[++i];
                } else if (args[i].equals("-sun")) { 
                    serIbis = false;                
                } else { 
                    System.out.println("Unknown option: " + args[i]);
                    System.exit(1);
                }
            }
        
            if (master) { 
                if (config == null) { 
                    System.out.println("Master requires config file!");
                    System.exit(1);
                }
            
                if (type != null) { 
                    System.out.println("Master ignoring preferred type!");
                }
            } else { 
                if (config != null) { 
                    System.out.println("Worker ignoring config file!");
                }
                
                if (type == null) { 
                    System.out.println("Worker requires type!");
                    System.exit(1);
                }
            }
            
            if (master) {
                Config c = new Config(config);
                c.parse();
                // c.print();
                
                // Start a master
                Master m = new Master(c, serIbis);
                m.start();
            } else { 
                // Start a worker
                Worker w = new Worker(type, serIbis);
                w.start();
            }
                    
        } catch (Exception e) {
            System.out.println("[*] Run failed!");
            e.printStackTrace();
        }
    }
}
