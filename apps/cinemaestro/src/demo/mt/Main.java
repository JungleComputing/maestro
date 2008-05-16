package demo.mt;

import image.ImageQueue;
import image.queues.RoundRobinInputQueue;
import image.queues.RoundRobinOutputQueue;
import image.queues.SimpleImageQueue;

import java.io.File;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;

import processors.ProcessorThread;
import processors.StatisticsCallback;
import util.config.ComponentDescription;
import util.config.Config;
import util.config.QueueDescription;

public class Main {

    private boolean verbose = true;
    
    private Config config;
    
    private LinkedList<ProcessorThread> threads = new LinkedList<ProcessorThread>();
    private HashMap<String, ImageQueue> queues = new HashMap<String, ImageQueue>(); 
    
    public Main(Config c) { 
        this.config = c;
    }

    private void addQueue(String name, ImageQueue q) {         
        System.out.println("-   Adding " + name + " to queue map");
        queues.put(name, q);        
    }
    
    @SuppressWarnings("unchecked")
    private void createQueue(QueueDescription q) throws Exception { 
        
        String [] put = q.getPut();
        String [] get = q.getGet();
        
        int len = q.getTotalLength();

        String name = q.getName();
        
        Class type = q.getType();
        
        System.out.println("Creating " + put.length + "-" + get.length 
                + " queue " + name + "[" + len + "]");
        
        if (put.length == 1) { 
            if (get.length == 1) { 
                SimpleImageQueue tmp = SimpleImageQueue.create(type, name, len); 
                addQueue(put[0], tmp);
                addQueue(get[0], tmp);
             } else { 
                RoundRobinOutputQueue tmp = RoundRobinOutputQueue.create(type, name, len, get.length);
          
                addQueue(put[0], tmp);
                
                for (int i=0;i<get.length;i++) { 
                    addQueue(get[i], tmp.getQueue(i));
                }
             }
        } else { 
            if (get.length == 1) { 
                RoundRobinInputQueue tmp = RoundRobinInputQueue.create(type, name, len, put.length); 
            
                for (int i=0;i<put.length;i++) { 
                    addQueue(put[i], tmp.getQueue(i));
                }
                
                addQueue(get[0], tmp);
                
            } else { 
                throw new Exception("Queue type not allowed!");
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    private ProcessorThread create(Class clazz, ComponentDescription c, 
            ImageQueue in, ImageQueue out) throws Exception {

        try {
            Method m = clazz.getDeclaredMethod("create", 
                    new Class [] { 
                    ComponentDescription.class, ImageQueue.class, 
                    ImageQueue.class, StatisticsCallback.class } );

            return (ProcessorThread) m.invoke(null, 
                    new Object [] { c, in, out, null }); 

        } catch (Exception e) {
            throw new Exception("Failed to create processor!", e);
        }
    }
    
    private void createComponent(ComponentDescription c) throws Exception { 
        
        Class<?> clazz = c.getClazz();
        HashMap<String, String> options = c.getOptions();
        
        String name = c.getName();

        String in = c.getInput();
        String out = c.getOutput();
        
        ImageQueue input = null;
        ImageQueue output = null;
        
        if (in != null) { 
            input = queues.get(in);
        }
        
        if (out != null) { 
            output = queues.get(out);
        }
        
        if (verbose) { 
            QueueDescription inQ = c.getGetQ();
            QueueDescription outQ = c.getPutQ();
            
            Class typeIn = (inQ != null ? inQ.getType() : null);
            Class typeOut = (outQ != null ? outQ.getType() : null);
            
            System.out.println("Create component " + name);
            System.out.println(" - Type     : " + clazz.getName());

            System.out.println(" - In type  : " + (typeIn != null ? typeIn.getName() : "none"));
            System.out.println(" - In name  : " + (inQ == null ? "none" : inQ.getName()));
            System.out.println(" - In ok    : " + (in == null || (in != null && input != null)));
            
            System.out.println(" - Out type : " + (typeOut != null ? typeOut.getName() : "none"));
            System.out.println(" - Out name : " + (outQ == null ? "none" : outQ.getName()));
            System.out.println(" - Out ok   : " + (out == null || (out != null && output != null)));

            if (options.size() > 0) { 
                System.out.println(" - Options  : " + options);
            }
        }
        
        ProcessorThread t = create(clazz, c, input, output);
        threads.add(t);
    }
    
    public void start() throws Exception { 

        LinkedList<QueueDescription> queues = config.getQueues();
        LinkedList<ComponentDescription> components = config.getComponents();
       
        System.out.println("Creating " + queues.size() + " queues");
        
        for (QueueDescription q : queues) { 
            createQueue(q);
        }
       
        System.out.println("Creating " + components.size() + " components");
        
        for (ComponentDescription c : components) { 
            createComponent(c);
        }
        
        System.out.println("Starting " + threads.size() + " processors");
        
        for (ProcessorThread t : threads) { 
            t.start();
        }
        
        System.out.println("Waiting for " + threads.size() + " processors");
         
        for (ProcessorThread t : threads) { 
            t.join();
        }
        
        System.out.println("Statistics:");

        for (ProcessorThread t : threads) { 
            t.printStatistics();
        }  
    }
    
    public static void main(String [] args) { 

        try { 
            Config c = new Config(new File(args[0]));
            c.parse();
            //c.print();

            new Main(c).start();        
        } catch (Exception e) {
            System.out.println("Single machine run failed!");
            e.printStackTrace();
        }
    }
}
