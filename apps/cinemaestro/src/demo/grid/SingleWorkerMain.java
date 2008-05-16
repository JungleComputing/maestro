package demo.grid;

import java.util.ArrayList;

public class SingleWorkerMain {

    public static void main(String [] args) {
        
        try { 
            int rank = -1;
            int size = -1;
            
            ArrayList<String> types = new ArrayList<String>();
            ArrayList<Integer> count = new ArrayList<Integer>();
            
            boolean serIbis = true;
            
            for (int i=0;i<args.length;i++) { 
                
                if (args[i].equals("-type")) { 
                    types.add(args[++i]);
                } else if (args[i].equals("-count")) { 
                    count.add(Integer.parseInt(args[++i]));
                } else if (args[i].equals("-sun")) { 
                    serIbis = false;                
                } else { 
                    rank = Integer.parseInt(args[i++]);
                    size = Integer.parseInt(args[i++]);
                }
            }
        
            int left = rank;
            String type = null;
            
            for (int i=0;i<count.size();i++) { 
                
                if (left < count.get(i)) {
                    type = types.get(i);
                    break;
                } else { 
                    left -= count.get(i);
                }
            }
            
            if (type == null) { 
                System.out.println("[*] EEP! " + types + " " + count + " " + rank + " " + size);
                System.exit(1);
            }
            
             // Start a worker
            Worker w = new Worker(type, serIbis);
            w.start();
                    
        } catch (Exception e) {
            System.out.println("[*]  Run failed!");
            e.printStackTrace();
        }
    }
}
