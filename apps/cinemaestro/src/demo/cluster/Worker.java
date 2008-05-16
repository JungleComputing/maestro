package demo.cluster;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import image.Image;
import image.ImageQueue;
import image.queues.RoundRobinInputQueue;
import image.queues.RoundRobinOutputQueue;
import image.queues.SimpleImageQueue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;

import net.Comm;
import net.IbisImageReader;
import net.IbisImageWriter;
import net.ManagementCallback;
import processors.ProcessorThread;
import processors.Statistics;
import processors.StatisticsCallback;
import util.config.ComponentDescription;
import util.config.QueueDescription;

public class Worker implements ManagementCallback, StatisticsCallback {

    private boolean verbose = true;

    private final Comm comm;

    private HashMap<String, ImageQueue<? extends Image>> queues = new HashMap<String, ImageQueue<?extends Image>>(); 

    private LinkedList<IbisImageWriter> writers = new LinkedList<IbisImageWriter>(); 
    private LinkedList<IbisImageReader> readers = new LinkedList<IbisImageReader>(); 

    private ComponentDescription workerComponent;
    private ProcessorThread thread; 
    
    public Worker() throws Exception { 
        comm = new Comm(false, this, true);
        comm.registerAtMaster(null);
    }

    public void managementMessage(IbisIdentifier id, Object data) {
        System.out.println("Got message from " + id + " data " + data);
    }

    public void register(IbisIdentifier id, Object data) {
        System.out.println("EEP: Worker got register message from " + id);
    }

    public synchronized void registered(IbisIdentifier id, Object data) {
        // TODO Auto-generated method stub
        System.out.println("Got registered message " + data);

        workerComponent = (ComponentDescription) data;
        notifyAll();  
    }

    public void publish(Statistics s) {

        // We got some statistics back from the component (presumably becaus it 
        // has nothing better to do at the moment). Let's forward them to the 
        // master.
        
        try {
            comm.sendManagementMessage(s);
        } catch (Exception e) {
            System.out.println("EEP: failed to send statistics!");
            e.printStackTrace();
        }        
    }
    
    /*
    private void startComponents() { 

        System.out.println("I now have a node for each component!");

        int index = 0;

        for (ComponentDescription c : componentDescriptions) { 

            IbisIdentifier target = nodes.get(index);

            try {
                comm.addManagementOut("node" + index, target, c);
            } catch (Exception e) {
                e.printStackTrace();
            }

            index++;
        }
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
    }*/

    @SuppressWarnings("unchecked")
    private ProcessorThread create(Class<?> clazz, String name, ImageQueue in, 
            ImageQueue out, HashMap<String, String> options) throws Exception {

        try {
            Method m = clazz.getDeclaredMethod("create", 
                    new Class [] { String.class, ImageQueue.class, 
                    ImageQueue.class, HashMap.class, StatisticsCallback.class } );

            return (ProcessorThread) m.invoke(null, 
                    new Object [] { name, in, out, options, this }); 

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

        ImageQueue<? extends Image>  input = null;
        ImageQueue<? extends Image> output = null;

        if (in != null) { 
            input = queues.get(in);
        }

        if (out != null) { 
            output = queues.get(out);
        }

        if (verbose) { 
            QueueDescription inQ = c.getGetQ();
            QueueDescription outQ = c.getPutQ();

            Class<? extends Image> typeIn = (inQ != null ? inQ.getType() : null);
            Class<? extends Image> typeOut = (outQ != null ? outQ.getType() : null);

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
    
        thread = create(clazz, name, input, output, options);
    }


    @SuppressWarnings("unchecked")
    private void createReader(String name, ImageQueue q) throws Exception { 

        //System.out.println("CREATING REC PORT " + name);

        ReceivePort p = comm.createDataReceivePort(name);
        IbisImageReader reader = new IbisImageReader(p, q);

        readers.add(reader);

        reader.start();

        comm.registerEndpoint(name);
    }

    @SuppressWarnings("unchecked")
    private void createWriter(String name, ImageQueue q) throws IOException { 

        //System.out.println("CREATING SEND PORT TO " + name);

        IbisIdentifier id = comm.find(name);
        SendPort p = comm.createDataSendPort(id, name);
        IbisImageWriter writer = new IbisImageWriter(q, p);

        writers.add(writer);

        writer.start();
    }

    @SuppressWarnings("unchecked")
    private void createPutQ(String name, QueueDescription putQ) throws IOException { 

        String [] gets = putQ.getGet();

        if (gets.length == 1) { 

            SimpleImageQueue q = SimpleImageQueue.create(putQ.getType(), "eep", 
                    putQ.getPutLength());

            createWriter(name + "->" + gets[0], q);
            queues.put(name, q);

        } else { 

            RoundRobinOutputQueue q = RoundRobinOutputQueue.create(putQ.getType(),
                    "eep", putQ.getGetLength(), gets.length);

            for (int i=0;i<gets.length;i++) { 
                createWriter(name + "->" + gets[i], q.getQueue(i));
            }

            queues.put(name, q);
        }

        //System.out.println("CREATED PUT QUEUE " + name);
    }

    @SuppressWarnings("unchecked")
    private void createGetQ(String name, QueueDescription getQ) throws Exception { 

        String [] puts = getQ.getPut();

        if (puts.length == 1) { 

            SimpleImageQueue q = SimpleImageQueue.create(getQ.getType(), "eep", 
                    getQ.getGetLength());

            createReader(puts[0] + "->" + name, q);
            queues.put(name, q);

        } else { 

            RoundRobinInputQueue q = RoundRobinInputQueue.create(getQ.getType(),
                    "eep", getQ.getGetLength(), puts.length);

            for (int i=0;i<puts.length;i++) { 
                createReader(puts[i] + "->" + name, q.getQueue(i));
            }

            queues.put(name, q);
        }

        // System.out.println("CREATED GET QUEUE " + name);


    }

    private synchronized void waitForComponent() { 
        while (workerComponent == null) { 
            try { 
                wait();
            } catch (Exception e) {
                // ignore
            }
        }    
    }

    public void start() throws Exception { 

        System.out.println("Waiting for work");

        waitForComponent();

        System.out.println("Got component " + workerComponent.getName());

        QueueDescription getQ = workerComponent.getGetQ();

        if (getQ != null) { 
            System.out.println("Need to create get queue " + workerComponent.getInput());
            System.out.println("  type " + getQ.getType().getName());
            System.out.println("  length " + getQ.getGetLength());
            System.out.println("  connected to " + Arrays.toString(getQ.getPut()));

            createGetQ(workerComponent.getInput(), getQ);
        }

        QueueDescription putQ = workerComponent.getPutQ();

        if (putQ != null) { 
            System.out.println("Need to create put queue " + workerComponent.getOutput());
            System.out.println("  type " + putQ.getType().getName());
            System.out.println("  length " + putQ.getGetLength());
            System.out.println("  connected to " + Arrays.toString(putQ.getGet()));

            createPutQ(workerComponent.getOutput(), putQ);
        }

        createComponent(workerComponent);

        System.out.println("Starting component " + workerComponent.getName());

        // NOTE: we run the component directly here, without starting a new thread!
        thread.run();
        
        
        thread.printStatistics();

        System.out.println(workerComponent.getName() + " is done ?");
    }
}
