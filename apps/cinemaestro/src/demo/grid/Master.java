package demo.grid;

import ibis.ipl.IbisIdentifier;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;

import net.Comm;
import net.ManagementCallback;
import processors.Statistics;
import util.FilesetDescription;
import util.config.ComponentDescription;
import util.config.Config;

public class Master implements ManagementCallback {

    private final Comm comm;
    private final Config config;

    private class TypedComponents { 

        final String name;
        final LinkedList<ComponentDescription> components = new LinkedList<ComponentDescription>();
        final LinkedList<IbisIdentifier> nodes = new LinkedList<IbisIdentifier>();

        public TypedComponents(final String name) {
            this.name = name;
        }

        public void add(ComponentDescription c) {
            components.add(c);
        }

        public synchronized void add(IbisIdentifier id) {

            if (nodes.size() == components.size()) { 
                System.out.println("EEP: got too many " + name + " nodes!");
            }

            nodes.add(id);
        }

        public int getComponentCount() {
            return components.size();
        }

        public synchronized int getNodeCount() {
            return nodes.size();
        }

        public LinkedList<ComponentDescription> getComponents() { 
            return components;
        }

        public LinkedList<IbisIdentifier> getNodes() { 
            return nodes;
        }

        public String getName() {
            return name;
        }

        public synchronized boolean full() {
            return nodes.size() >= components.size();
        }
    }

    private class NodeInfo { 

        final IbisIdentifier node;
        final FilesetDescription fs; 
        ListingReply reply;

        public NodeInfo(final IbisIdentifier node, final FilesetDescription fs) {
            this.node = node;
            this.fs = fs;
        }

        public synchronized void setReply(ListingReply reply) { 
            this.reply = reply;
            notifyAll();
        }

        public synchronized ListingReply getReply() {

            long time = 0;

            while (reply == null) { 
                try { 
                    wait(1000);
                    time += 1000;
                } catch (Exception e) {
                    // ignore
                }

                if (reply == null && time >= 60000) {
                    return null;
                }                
            }

            return reply;
        }
    }

    // private HashMap<IbisIdentifier, Object> nodes = new HashMap<IbisIdentifier, Object>();

    private final HashMap<String, TypedComponents> types = new HashMap<String, TypedComponents>();
    private final HashMap<String, FilesetDescription> filesets;

    private final Hashtable<IbisIdentifier, NodeInfo> nodeinfo = new Hashtable<IbisIdentifier, NodeInfo>();

    private int componentsStarted = 0;
    private int componentsDone = 0;

    private Statistics [] latestInfo;

    public Master(Config c, boolean serIbis) throws Exception { 
        this.config = c;
        sortDescriptions(config.getComponents());
        filesets = config.getFilesets();
        comm = new Comm(true, this, serIbis);
    }

    private void sortDescriptions(LinkedList<ComponentDescription> cds) { 

        latestInfo = new Statistics[cds.size()];

        for (ComponentDescription c : cds) { 

            String name = c.getName();

            TypedComponents t = types.get(name);

            if (t == null) { 
                t = new TypedComponents(name);
                types.put(name, t);
            }

            t.add(c);
        }

        System.out.println("This configuration requires:");

        for (TypedComponents t : types.values()) { 
            System.out.println("Component " + t.getName() + " " + t.getComponentCount());
        }
    }

    private synchronized void processStatistics(Statistics s) { 
        latestInfo[s.getNumber()] = s;

        if (s.isDone()) { 
            componentsDone++;
            notifyAll();
        }
    }

    public void managementMessage(IbisIdentifier id, Object data) {
        if (data instanceof Statistics) { 
            processStatistics((Statistics) data);
        } else if (data instanceof ListingReply) {        
            NodeInfo info = nodeinfo.get(id);
            info.setReply((ListingReply) data);
        } else { 
            System.out.println("Got unknown message " + data);
        }
    }

    public void register(IbisIdentifier id, Object data) {

        System.out.println("Got register message from " + id);

        if (data == null) { 
            System.out.println("EEP: incomplete register message!");
            return;
        }

        TypedComponents t = types.get((String) data);

        if (t == null) { 
            System.out.println("EEP: type " + data + " not found!");
            return;
        } 

        t.add(id);

        System.out.println("Node registered of type: " + t.getName());
    }

    public void registered(IbisIdentifier id, Object data) {
        // TODO Auto-generated method stub
        System.out.println("EEP: Master got registered message! " + data);
    }


    private FilesetDescription getFileSets(HashMap<String, String> options) throws Exception { 

        for (String value : options.values()) { 

            if (value.startsWith("fileset://")) { 

                String name = value.substring(10);

                FilesetDescription tmp = filesets.get(name);

                if (tmp == null) { 
                    throw new Exception("FileSet " + name +" not found!");
                }

                return tmp;
            }

        }

        return null;
    }

    private void requestListing(IbisIdentifier id, FilesetDescription fs) throws Exception { 
        comm.connectToWorker(id);
        nodeinfo.put(id, new NodeInfo(id, fs));
        comm.sendManagementMessage(id, new ListingRequest(fs));
    }

    private ListingReply getListingReply(IbisIdentifier id) throws Exception {         
        NodeInfo n = nodeinfo.get(id);

        ListingReply r = n.getReply();

        if (r == null) { 
            throw new Exception("Failed to get listing reply from " + id);
        }

        if (!r.isValid()) { 
            throw new Exception("Got invalid listing reply from " + id + ": " + r);
        }

        return r; 
    }

    private void startByFileSet(TypedComponents t, FilesetDescription fs) 
    throws Exception {

        // this components requires a fileset as input...
        System.out.println("Starting component " + t.getName() + " using fileset");

        // Request a listing on all nodes 
        LinkedList<IbisIdentifier> nodes = t.getNodes();

        for (IbisIdentifier id : nodes) { 
            requestListing(id, fs);
        }

        // Get all listing replies
        ListingReply [] replies = new ListingReply[nodes.size()];

        int index = 0;

        for (IbisIdentifier id : nodes) {
            replies[index++] = getListingReply(id);
        }

        // Check if all replies have matching start, end and stride values
        if (replies.length == 0) {
            throw new Exception("No replies to listing request");
        }

        Arrays.sort(replies);

        long stride = replies[0].getStride();

        System.out.println("Stride seems to be " + stride);

        if (stride == 1) { 

            // All machines should have the same set
            long start = replies[0].getStart();
            long end = replies[0].getEnd();

            for (int i=1;i<replies.length;i++) {

                if (replies[i].getStart() != start) { 
                    throw new Exception("Invalid file start on " + replies[i].getID());
                }

                if (replies[i].getEnd() != end) { 
                    throw new Exception("Invalid file end on " + replies[i].getID());
                }

                if (replies[i].getStride() != 1) { 
                    throw new Exception("Invalid file stride on " + replies[i].getID());
                }
            }   

            // Since everyone has the same fileset, we'll just start in any 
            // order

            System.out.println("Starting normally");

            Iterator <ComponentDescription> components = t.getComponents().iterator();
            int number = 0;

            for (ListingReply r : replies) {

                ComponentDescription c = components.next(); 
                //c.setNumber(componentsStarted++);
                componentsStarted++;
                
                try {
                    comm.registerWorker(r.getID(), c);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                number++;
            }

        } else { 

            // We have a distributed set. 
            if (replies.length != stride) { 
                throw new Exception("Stride " + stride + " does not match number of machines " + replies.length);
            }

            long start = replies[0].getStart();
            long end = replies[0].getEnd();

            for (int i=1;i<replies.length;i++) {

                if (replies[i].getStride() != stride) { 
                    throw new Exception("Invalid file stride on " + replies[i].getID());
                }

                if (replies[i].getStart() != start+i) { 
                    throw new Exception("Invalid file start on " + replies[i].getID());
                }

                long tmp = replies[i].getEnd();

                if (!(tmp == (end+i) || tmp == (end-stride+i))) { 
                    throw new Exception("Invalid file end on " + replies[i].getID());
                }
            }   

            // System.out.println("Starting re-ranked!");

            // We now start the components on the right nodes by looking at 
            // what part of the fileset they contain.
            LinkedList<ComponentDescription> components = t.getComponents();
            // int number = 0;

            for (ListingReply r : replies) {

                int rank = (int) r.getStart();

                boolean found = false;

                for (ComponentDescription c : components) { 

                    if (c.getRank() == rank) { 
                        try {
                            comm.registerWorker(r.getID(), c);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        found = true;
                        break;
                    }
                }

                if (!found){ 
                    System.out.println("EEP: component " + t.getName() 
                            + " with rank " + r.getStart() + " not found!");
                }
                
                componentsStarted++;
                
                /*
                ComponentDescription c = components.next();                 
                c.setRank(number);
                c.setSize(t.getNodeCount());
                c.setNumber(componentsStarted++);

                System.out.println("Component " + c.getName() + " now has rank "
                        + c.getRank() + " and size " + c.getSize());

                try {
                    comm.registerWorker(r.getID(), c);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                number++;
                 */
            }
        }
    }

    private void deployComponents(TypedComponents t) throws Exception {

        // Note: unless shit is hitting fan, all ComponentDescription in this 
        // t should be based on the same description. The only difference should 
        // be queue names, ranks, etc.

        // Check if this component depends on a fileset
        ComponentDescription tmp = t.getComponents().getFirst();
        FilesetDescription fs = tmp.getFileSet();

        if (fs != null && fs.getCheck()) {
            // If so, take this set into account when starting 
            startByFileSet(t, fs);
        } else { 
            // If not, start directly in any available order            
            Iterator<IbisIdentifier> nodes = t.getNodes().iterator();
            Iterator <ComponentDescription> components = t.getComponents().iterator();

            int index = 0;

            while (nodes.hasNext()) { 

                IbisIdentifier n = nodes.next();

                ComponentDescription c = components.next(); 
                componentsStarted++;
                

                try {
                    comm.connectToWorker(n);
                    comm.registerWorker(n, c);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                index++;
            }
        }    
    }

    private void deploy() throws Exception { 

        System.out.println("I now have a node for each component!");

        for (TypedComponents t : types.values()) { 
            deployComponents(t);
        }
    }

    private void startComponents(TypedComponents t) throws Exception { 

        for (IbisIdentifier i : t.getNodes()) {
            comm.sendManagementMessage(i, true);
        }
    }
    
    private void startComponents() throws Exception { 

        for (TypedComponents t : types.values()) { 
            System.out.println("Starting " + t.getName());
            startComponents(t);
        }
    }
    
    
    private void waitForNodes() { 

        System.out.println("Waiting for all nodes");

        boolean done = false;

        while (!done) { 

            try { 
                Thread.sleep(1000);
            } catch (Exception e) {
                // ignore
            }

            done = true;

            for (TypedComponents t : types.values()) {

                boolean tmp = t.full();

                System.out.println("Group " + t.getName() + " " + t.getNodeCount());

                done = done & tmp;
            }
        }
    }

    private synchronized boolean getDone() {
        
        System.out.println(componentsDone + " "  + componentsStarted);
        
        return componentsDone == componentsStarted;
    }

    private synchronized Statistics getStatistics(int index) {
        return latestInfo[index];
    }
    
    private String sizeToHR(long size) { 

        String unit = null;
        double result = -1.0;

        if (size > 1024L*1024L*1024L) { 
            unit = "GB";
            result = size / (1024.0*1024.0*1024.0);
        } else if (size > 1024L*1024L) {
            unit = "MB";
            result = size / (1024.0*1024.0);
        } else if (size > 1024L) { 
            unit = "KB";
            result = size / 1024.0; 
        } else { 
            unit = "B";
            result = size; 
        }

        return String.format("%.2f " + unit, result);
    }

    private String rateToHR(long size) { 

        String unit = null;
        double result = -1.0;

        if (size > 1000L*1000L*1000L) { 
            unit = "GBit";
            result = size / (1000.0*1000.0*1000.0);

        } else if (size > 1000L*1000L) {
            unit = "MBit";
            result = size / (1000.0*1000.0);
        } else if (size > 1000L) { 
            unit = "KBit";
            result = size / 1000.0; 
        } else { 
            unit = "Bit";
            result = size; 
        }

        return String.format("%.2f " + unit, result);
    }

    private void printEndStats(long total) { 

        for (String name : types.keySet()) { 

            long totalSizeIn = 0; 
            long totalSizeOut = 0; 
            long totalTime = 0;

            long idle = 0; 
            long processing = 0;

            int count = 0;

            int images = 0;

            for (Statistics s : latestInfo) { 

                if (s.getName().equals(name)) { 

                    images += s.getImages();
                    totalSizeIn += s.getSizeIn();
                    totalSizeOut += s.getSizeOut();
                    totalTime += s.getTotalTime();
                    idle += s.getIdleTime();
                    processing += s.getProcessingTime();

                    count++;
                }
            }

            long avgTime = (totalTime / count);

            System.out.println("Type           : " + name);
            System.out.println("Images         : " + images);
            System.out.println("Total size in  : " + sizeToHR(totalSizeIn));

            if (totalTime > 0) { 
                System.out.println("Rate in        : " + rateToHR((totalSizeIn*8*1000) / avgTime));
            }

            System.out.println("Total size out : " + sizeToHR(totalSizeOut));

            if (totalTime > 0) { 
                System.out.println("Rate out       : " + rateToHR((totalSizeOut*8*1000) / avgTime));
            }

            System.out.println("Average time   : " + avgTime + " ms.");
            System.out.println("  Idle         : " + (idle / count) + " ms.");
            System.out.println("  Processing   : " + (processing / count) + " ms.");
            System.out.println();

        }
    }

    private void waitForComponents() { 

        boolean done = getDone();

        long start = System.currentTimeMillis();

        while (!done) { 
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                // ignore
            }

            long now = System.currentTimeMillis();

            System.out.println("Info after " + ((now-start)/1000) + " sec.");

            for (int i=0;i<latestInfo.length;i++) { 
                Statistics s = getStatistics(i);

                if (s != null) { 
                    s.printStatistics();
                }
            }

            done = getDone();
        }

        long end = System.currentTimeMillis();


        System.out.println("=========================================\n\n\n");
        System.out.println("Total computation took " + ((end-start)/1000) + " sec.\n\n\n");

        for (int i=0;i<latestInfo.length;i++) { 
            Statistics s = getStatistics(i);

            if (s != null) {
                s.printStatistics();
            }
            System.out.println("\n\n\n");
        }

        System.out.println("=========================================\n\n\n");

        printEndStats(end-start);


    }

    public void start() throws Exception { 

        waitForNodes();

        deploy();

        startComponents();
        
        waitForComponents();
        
        comm.close();

    }




}
