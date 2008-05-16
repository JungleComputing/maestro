package util.config;

import image.Image;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import util.FilesetDescription;

public class Config {

    private static final int DEFAULT_IN_QUEUE_LENGTH = 1;
    private static final int DEFAULT_OUT_QUEUE_LENGTH = 1;

    private static final String COMPONENT_KEY = "component";
    private static final String FILESET_KEY   = "fileset";
    private static final String CLUSTER_KEY   = "cluster";
    private static final String PIPELINE_KEY  = "pipeline";
    private static final String OPTIONS_KEY   = "options";
    private static final String DEPLOY_KEY    = "deploy";

    private static final Pattern hasInAndOutQueue = Pattern.compile("\\d*\\:\\w*:\\d*");
    private static final Pattern hasInQueue = Pattern.compile("\\d*\\:\\w*");
    private static final Pattern hasOutQueue = Pattern.compile("\\w*:\\d*");

    private HashMap<String, Component> components = new HashMap<String, Component>();
    private HashMap<String, Cluster> clusters = new HashMap<String, Cluster>();
    private HashMap<String, Pipeline> pipelines = new HashMap<String, Pipeline>();

    private HashMap<String, QueueDescription> queueMap = new HashMap<String, QueueDescription>(); 
    
    private HashMap<String, FilesetDescription> filesets = new HashMap<String, FilesetDescription>();
    private LinkedList<QueueDescription> queues = new LinkedList<QueueDescription>(); 
    private LinkedList<ComponentDescription> descriptions = new LinkedList<ComponentDescription>();

    private Pipeline deploy;

    private BufferedReader reader;

    private int componentNumber = 0;

    private int lineNumber = 0;
    private String current;

    private boolean processed = false;

    public Config(File config) throws FileNotFoundException { 
        reader = new BufferedReader(new FileReader(config));
    }

    private Class lookupClass(String name) throws Exception { 

        try {
            return Class.forName(name);
        } catch (ClassNotFoundException e) {
            throw new Exception("Failed to find class " + name);
        }
    }

    private void addComponent(String name, String className) 
    throws Exception { 

        if (components.containsKey(name)) { 
            throw new Exception("Component " + name + " already exists!");
        }

        System.out.println("Adding component " + name);

        Class clazz = lookupClass(className);

        Class<? extends Image> input = getInputType(clazz);
        Class<? extends Image> output = getOutputType(clazz);

        Component c = new Component(name, clazz, input, output);

        components.put(name, c);        
    }

    public Class<? extends Image> getInputType(Class clazz) { 

        try {
            Method m = clazz.getDeclaredMethod("getInputQueueType", (Class[]) null);
            return (Class) m.invoke(null, (Object[])null); 
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    public Class getOutputType(Class clazz) {

        try {
            Method m = clazz.getDeclaredMethod("getOutputQueueType", (Class[]) null);
            return (Class) m.invoke(null, (Object[])null); 
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return null;
    }


    private void parseComponent(String line) throws Exception { 

        StringTokenizer t = new StringTokenizer(line);

        if (t.countTokens() != 2) { 
            throw new Exception("Parse error on line: " + lineNumber + " - " + current);
        }

        String name = t.nextToken();
        String className = t.nextToken();

        addComponent(name, className);
    }

    private void parseCluster(String line) throws Exception { 

        StringTokenizer t = new StringTokenizer(line);

        if (t.countTokens() < 5) { 
            throw new Exception("Parse error on line: " + lineNumber + " - " + current);
        }

        String name = t.nextToken();

        if (clusters.get(name) != null) { 
            throw new Exception("Cluster " + name + " already defined!");
        }

        String location = null;
        int size = 0;
        Component c = null;

        while (t.hasMoreTokens()) { 

            String tmp = t.nextToken();

            if (tmp.startsWith("size")) { 

                if (size != 0) { 
                    throw new Exception("Size already set!");
                }

                if (!t.hasMoreTokens()) { 
                    throw new Exception("Parse error on line: " + lineNumber + " - " + current);
                }

                String s = t.nextToken();

                if (s.equals("*")) { 
                    size = -1;
                } else { 
                    size = Integer.parseInt(s);
                }

            } else if (tmp.startsWith("location")) { 

                if (location != null) { 
                    throw new Exception("Location already set!");
                }

                if (!t.hasMoreTokens()) { 
                    throw new Exception("Parse error on line: " + lineNumber + " - " + current);
                }

                location = t.nextToken();

                if (location.startsWith("[")) { 

                    while (!location.endsWith("]")) { 

                        if (!t.hasMoreTokens()) { 
                            throw new Exception("Parse error on line: " + lineNumber + " - " + current);
                        }

                        location += t.nextToken();
                    }
                }

            } else if (tmp.startsWith("component")) {

                if (!t.hasMoreTokens()) { 
                    throw new Exception("Parse error on line: " + lineNumber + " - " + current);
                }

                String component = t.nextToken();

                c = components.get(component);

                if (c == null) { 
                    throw new Exception("Component " + component + " not found!");
                }

            } else { 
                throw new Exception("Parse error on line: " + lineNumber + " - " + current);
            }
        }

        if (location == null && size == 0) { 
            throw new Exception("Either size OR location are required!");
        }

        Cluster cluster = new Cluster(name, size, location, c);
        clusters.put(name, cluster);
    }

    private void addOption(Component c, String option) throws Exception { 

        int index = option.indexOf("=");

        if (index < 0) { 
            throw new Exception("Failed to parse option: " + option);
        }

        String key = option.substring(0, index);
        String value = option.substring(index+1);

        if (c.getOption(key) != null) { 
            throw new Exception("Component already contains option: " + option);
        }

        c.addOption(key, value);
    }

    private void parseOptions(String line) throws Exception { 

        StringTokenizer t = new StringTokenizer(line);

        if (t.countTokens() < 1) { 
            throw new Exception("Parse error on line: " + lineNumber + " - " + current);
        }

        String name = t.nextToken();

        Component c = components.get(name);

        if (c == null) { 
            throw new Exception("Component " + name + " not found!");
        }

        while (t.hasMoreTokens()) { 
            addOption(c, t.nextToken());
        }
    }

    private Stage parseStage(String tmp, int width) throws Exception { 

        // NOTE: tmp can either be a component, or the name of a previously 
        // defined pipeline

        int get = DEFAULT_IN_QUEUE_LENGTH;
        int put = DEFAULT_OUT_QUEUE_LENGTH;

        if (hasInAndOutQueue.matcher(tmp).matches()) {

            int i1 = tmp.indexOf(":");
            int i2 = tmp.lastIndexOf(":");

            String lenGet = tmp.substring(0, i1);
            String lenPut = tmp.substring(i2+1);

            tmp = tmp.substring(i1+1, i2);

            get = Integer.parseInt(lenGet);
            put = Integer.parseInt(lenPut);

            System.out.println("Get and put queue! " + get + " " + tmp + " " + put);
        } else if (hasInQueue.matcher(tmp).matches()) {

            int index = tmp.indexOf(":");

            String len = tmp.substring(0, index);
            tmp = tmp.substring(index+1);

            System.out.println("Get queue! " + len + " " + tmp);

            get = Integer.parseInt(len);
        } else if (hasOutQueue.matcher(tmp).matches()) {

            int index = tmp.indexOf(":");

            String len = tmp.substring(index+1);
            tmp = tmp.substring(0, index);

            System.out.println("Put queue! " + tmp + " " + len);

            put = Integer.parseInt(len);
        } else { 
            System.out.println("No queue! " + tmp); 
        }

        Component c = components.get(tmp);

        if (c == null) {
            throw new Exception("Component " + tmp + " unknown");
        }

        return new Stage(c, width, get, put);
    } 

    private void parsePipeline(String line) throws Exception { 

        if (line == null || line.length() == 0) { 
            return;
        }

        StringTokenizer t = new StringTokenizer(line);

        int tokens = t.countTokens();

        if (tokens < 3) { 
            throw new Exception("Parse error on line: " + lineNumber + " - " + current);
        }

        String name = t.nextToken();

        if (pipelines.get(name) != null) { 
            throw new Exception("Pipeline " + name + " already exists");
        }

        Pipeline p = new Pipeline(name);

        for (int i=1;i<tokens;i+=2) {
            int width = Integer.parseInt(t.nextToken());
            p.addStage(parseStage(t.nextToken(), width));
        }

        pipelines.put(name, p);
    }

    private void parseDeploy(String line) throws Exception { 

        if (line == null || line.length() == 0) { 
            throw new Exception("Deploy line empty!");
        }

        deploy = pipelines.get(line);

        if (deploy == null) { 
            throw new Exception("Cannot deploy " + line);
        }
    }

    private void parseFileSet(String line) throws Exception { 

        if (line == null || line.length() == 0) { 
            throw new Exception("Fileset line empty!");
        }

        StringTokenizer t = new StringTokenizer(line);

        int tokens = t.countTokens();

        if (tokens != 3) { 
            throw new Exception("Parse error on line: " + lineNumber + " - " 
                    + current);
        }

        String required = t.nextToken();
        String name = t.nextToken();
        String file = t.nextToken();

        String dir = ".";
        String prefix = null;
        String postfix = null;

        int index = file.lastIndexOf(File.separator);

        if (index >= 0) { 
            dir = file.substring(0, index);
            file = file.substring(index+1);
        }

        int first = file.indexOf("%");
        int last  = file.lastIndexOf("%");

        if (first == -1) { 
            prefix = file; 
            postfix = "";
        } else { 
            prefix = file.substring(0, first);
            postfix = file.substring(last+1);
        }

        int positions = 1 + (last-first); 

        System.out.println("Parsed fileset dir=" + dir + " prefix=" + prefix + " postfix="  + postfix + " pos=" + positions);

        boolean check = false;

        if (required.equals("check")) { 
            check = true;
        } else if (required.equals("nocheck")) {
            check = false;
        } else { 
            throw new Exception("Parse error on line: " + lineNumber + " - " 
                    + current);
        }

        filesets.put(name, new FilesetDescription(check, dir, prefix, postfix, positions));

    }

    private void parseLine(String line) throws Exception { 

        if (line == null || line.length() == 0 || line.startsWith("#")) { 
            return;
        }

        if (line.startsWith(COMPONENT_KEY)) { 
            parseComponent(line.substring(COMPONENT_KEY.length()).trim());
        } else if (line.startsWith(CLUSTER_KEY)) { 
            parseCluster(line.substring(CLUSTER_KEY.length()).trim());
        } else if (line.startsWith(OPTIONS_KEY)) { 
            parseOptions(line.substring(OPTIONS_KEY.length()).trim());
        } else if (line.startsWith(PIPELINE_KEY)) { 
            parsePipeline(line.substring(PIPELINE_KEY.length()).trim());
        } else if (line.startsWith(DEPLOY_KEY)) { 
            parseDeploy(line.substring(DEPLOY_KEY.length()).trim());
        } else if (line.startsWith(FILESET_KEY)) { 
            parseFileSet(line.substring(FILESET_KEY.length()).trim());
        } else { 
            throw new Exception("Parse error on line: " + lineNumber + " - " + current);
        }
    }

    public void parse() throws Exception { 

        String tmp = reader.readLine();

        while (tmp != null) { 
            current = tmp;
            parseLine(tmp.trim());
            tmp = reader.readLine();
            lineNumber++;
        }       

        reader.close();
    }


    private void createComponents(Stage s, int stage, 
            LinkedList<ComponentDescription> result) throws Exception { 

        Component c = s.getComponent();

        for (int i=0;i<s.getWidth();i++) { 

            String get = null;
            String put = null;

            QueueDescription getQ = null;
            QueueDescription putQ = null;

            if (c.hasInputQueue()) { 
                get = "get." + stage + "." + i;
                getQ = getQueue(get);
            }

            if (c.hasOutputQueue()) { 
                put = "put." + stage + "." + i;
                putQ = getQueue(put);
            }
            
            ComponentDescription cd = null;
            
            if (c.usesFileset()) { 
                cd = new ComponentDescription(
                        componentNumber++, c, stage, i, s.getWidth(), get, put, 
                        getQ, putQ, filesets);
            } else { 
                cd = new ComponentDescription(
                        componentNumber++, c, stage, i, s.getWidth(), get, put, 
                        getQ, putQ, null);
            }
            result.add(cd);
        }        
    }

    public void processComponents() throws Exception { 

        Stage [] stages = deploy.getStages();

        for (int i=0;i<stages.length;i++) { 
            createComponents(stages[i], i, descriptions);
        }
    }

    private void processQueues() throws Exception { 

        Stage [] stages = deploy.getStages();

        for (int i=0;i<stages.length-1;i++) { 
            getQueues(stages[i], stages[i+1], i);
        }
    }

    private void process() throws Exception { 

        processQueues();
        processComponents();
        
        processed = true;
    }

    public LinkedList<ComponentDescription> getComponents() throws Exception { 

        if (!processed) { 
            process();
        }

        return descriptions;
    }

    public LinkedList<QueueDescription> getQueues() throws Exception { 

        if (!processed) { 
            process();
        }

        return queues;
    }
    
    private void addQueue(String [] names, QueueDescription q) throws Exception { 

        System.out.println("Adding queue description: " + q.getName());

        for (String s : names) { 

            if (queueMap.containsKey(s)) { 
                throw new Exception("Queue endpoint " + s + " already exists!");
            }

            System.out.println("Adding queue endpoint: " + s);

            queueMap.put(s, q);
        }

        queues.add(q);
    }

    private QueueDescription getQueue(String name) throws Exception { 

        if (!queueMap.containsKey(name)) { 
            throw new Exception("Queue not found: " + name);
        }

        return queueMap.get(name);

    }

    @SuppressWarnings("unchecked")
    private void getQueues(Stage from, Stage to, int stage) throws Exception { 

        int fromWidth = from.getWidth();
        int toWidth   = to.getWidth();

        if (fromWidth == 0) { 
            throw new Exception("Stage " + from.getComponent().getName() 
                    + " has no outputs!");
        }

        if (toWidth == 0) { 
            throw new Exception("Stage " + to.getComponent().getName() 
                    + " has no inputs!");
        }

        Class typeOut = from.getComponent().getOutputType();
        Class typeIn = to.getComponent().getInputType();

        if (!typeIn.isAssignableFrom(typeOut)) { 
            throw new Exception("Output of " + from.getComponent().getName() 
                    + " does not match input of " + to.getComponent().getName()
                    + " (" + typeOut.getName() + " != " + typeIn.getName() + ")");
        }

        if (fromWidth == toWidth) { 
            // We have a number of 1-to-1 queues here

            for (int i=0;i<fromWidth;i++) { 

                String name = from.getComponent().getName() + "[" + i + "]->" 
                + to.getComponent().getName() + "[" + i + "]";  

                String [] put = new String [] { "put." + stage + "." + i };
                String [] get = new String [] { "get." + (stage+1) + "." + i };

                QueueDescription qd = new QueueDescription(name, typeIn, put, 
                        get, from.getPutQueueLength(), to.getGetQueueLength());

                addQueue(put, qd);
                addQueue(get, qd);
            }

        } else if (fromWidth < toWidth) { 

            // We have a number of 1-to-N queues. 

            if (toWidth % fromWidth != 0) { 
                throw new Exception("Cannot create a " + fromWidth + " to " 
                        + toWidth + " connection");
            }

            int n = toWidth / fromWidth;

            for (int i=0;i<fromWidth;i++) { 

                String name = from.getComponent().getName() + "[" + i + "]->" 
                + to.getComponent().getName() + "[";

                String [] put = new String [] { "put." + stage + "." + i };

                String [] get = new String[n];

                int target = i;

                for (int j=0;j<n;j++) { 

                    name += target;

                    if (j != n-1) { 
                        name += ",";  
                    }   

                    get[j] = "get." + (stage+1) + "." + target;

                    target += fromWidth;
                }

                name += "]";  

                QueueDescription qd = new QueueDescription(name, typeIn, put, 
                        get, from.getPutQueueLength(), to.getGetQueueLength());

                addQueue(put, qd);
                addQueue(get, qd);
            }

        } else { // fromWidth > toWidth 

            // We have a number of N-to-1 queues. 

            if (fromWidth % toWidth != 0) { 
                throw new Exception("Cannot create a " + fromWidth + " to " 
                        + toWidth + " connection");
            }

            int n = fromWidth / toWidth;

            for (int i=0;i<toWidth;i++) { 

                String name = from.getComponent().getName() + "["; 


                String [] put = new String[n];

                int source = i;

                for (int j=0;j<n;j++) { 

                    name += source;

                    if (j != n-1) { 
                        name += ",";  
                    }   

                    put[j] = "put." + (stage) + "." + source;

                    source += toWidth;
                }

                name += "]->" + to.getComponent().getName() + "[" + i + "]";

                String [] get = new String [] { "get." + (stage+1) + "." + i };

                QueueDescription qd = new QueueDescription(name, typeIn, put, 
                        get, from.getPutQueueLength(), to.getGetQueueLength());

                addQueue(put, qd);
                addQueue(get, qd);
            }
        }
    }


    public HashMap<String, FilesetDescription> getFilesets() { 
        return filesets;
    }

    public Pipeline getDeploy() { 
        return deploy;
    }

    public static void main(String [] args) { 

        try {
            Config c = new Config(new File(args[0]));
            c.parse();

            Pipeline p = c.getDeploy();

            System.out.println("Deploy: " + p.getName());

            System.out.println("Lenght: " + p.getLength());

            for (Stage s : p.getStages()) { 
                System.out.println(s.getWidth() + " x " + s.getComponent().getName());
            }

            //   c.print();

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


}
