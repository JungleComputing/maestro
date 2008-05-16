package demo.cluster;

import ibis.ipl.IbisIdentifier;

import java.util.ArrayList;
import java.util.LinkedList;

import net.Comm;
import net.ManagementCallback;
import processors.Statistics;
import util.config.ComponentDescription;
import util.config.Config;

public class Master implements ManagementCallback {
    
    private final Comm comm;
    private final Config config;
    
    // private LinkedList<QueueDescription> queueDescriptions;
    private LinkedList<ComponentDescription> componentDescriptions;
    
    private ArrayList<IbisIdentifier> nodes = new ArrayList<IbisIdentifier>();
    
    public Master(Config c) throws Exception { 
        this.config = c;
        // queueDescriptions = config.getQueues();
        componentDescriptions = config.getComponents();
        comm = new Comm(true, this, true);
    }
    
    public void managementMessage(IbisIdentifier id, Object data) {
        if (data instanceof Statistics) { 
            Statistics s = (Statistics) data;
            s.printStatistics();
        } else { 
            System.out.println("Got unknown message " + data);
        }
    }

    public void register(IbisIdentifier id, Object data) {
        
        System.out.println("Got register message from " + id);
        
        synchronized (nodes) {
            nodes.add(id);
            nodes.notifyAll();
        }
    }

    public void registered(IbisIdentifier id, Object data) {
        // TODO Auto-generated method stub
        System.out.println("EEP: Master got registered message! " + data);
    }
    
    private void startComponents() { 
    
        System.out.println("I now have a node for each component!");
        
        int index = 0;
        
        for (ComponentDescription c : componentDescriptions) { 
            
            IbisIdentifier target = nodes.get(index);
            
            try {
                comm.registerWorker(target, c);
            } catch (Exception e) {
                e.printStackTrace();
            }
            
            index++;
        }
    }

    private void waitForNodes(int number) { 
        
        System.out.println("Waiting for " + number + " nodes");
        
        synchronized (nodes) {            
            while (nodes.size() < number) { 
                try { 
                    nodes.wait();
                } catch (Exception e) {
                    // ignore
                }
            }            
        }
        
        
    }
    
    public void start() throws Exception { 
        
        waitForNodes(componentDescriptions.size());
        
        startComponents();
        
        // TODO: decent exit
        
        while (true) { 
            Thread.sleep(10000);
        }
    }

    

   
}
