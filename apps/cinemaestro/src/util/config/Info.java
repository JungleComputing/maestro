package util.config;

import java.util.ArrayList;
import java.util.LinkedList;

class Info { 
    
    private final String name;

    private final Stage stage;

    private final Info parent;             

    private final ArrayList<Info> [] childeren;
    
    private final int inQueueLength;
    private final int outQueueLength;
    
    @SuppressWarnings("unchecked")
    public Info(String name, Stage stage, int width, Info parent) { 
        
        this.name = name;
        this.stage = stage;
        this.parent = parent;            
        
        this.inQueueLength = -1;
        this.outQueueLength = -1; 
        
        if (width <= 0) {
            throw new RuntimeException("Width must be >= 1");
        }
        
        childeren = new ArrayList[width];
    }
    
    public Info(String name, Stage stage, int inQueueLength, 
            int outQueueLength, Info parent) { 
        
        this.name = name;
        this.stage = stage;
        this.parent = parent;            
        
        this.inQueueLength = inQueueLength;
        this.outQueueLength = outQueueLength;
        
        System.out.println("INFO " + name + " " + inQueueLength + " " + outQueueLength);
        
        childeren = null;
    }

    public int getWidth() {
        if (childeren == null) { 
            return 1;
        }
        
        return childeren.length;
    }
    
    public int getInQueueLength() { 
        
        if (inQueueLength == -1) { 
            return childeren[0].get(0).getInQueueLength();
        }
        
        return inQueueLength;
    }
        
    public int getOutQueueLength() { 
        
        if (outQueueLength == -1) { 
            return childeren[0].get(childeren[0].size()-1).getInQueueLength();
        }
        
        return outQueueLength;
    }
    
    public int getInQueueWidth() { 
        
        if (childeren == null) {
            return 1;
        }   
        
        int w = 0;
            
        for (int i=0;i<childeren.length;i++) { 
            w += childeren[i].get(0).getInQueueWidth();
        }

        return w;
    }
        
    public int getOutQueueWidth() { 

        if (childeren == null) {
            return 1;
        }   
        
        int w = 0;
            
        for (int i=0;i<childeren.length;i++) { 
            w += childeren[i].get(0).getOutQueueWidth();
        }

        return w;
    }
    
    public ArrayList<Info> getChilderen(int index) {
        return childeren[index];
    }

    public void setChilderen(int index, ArrayList<Info> childeren) {
        this.childeren[index] = childeren;
    }

    public String getName() {
        return name;
    }

    public Info getParent() {
        return parent;
    }

    public Stage getStage() {
        return stage;
    }

    public Class inQueueType() {
        return stage.getComponent().getInputType();                
    }
    
    public Class outQueueType() {
        return stage.getComponent().getOutputType();                
    }
    
    public LinkedList<String> inQueueNames() { 
        
        LinkedList<String> t = new LinkedList<String>();
        
        if (childeren == null) {                
            if (stage.getComponent().getInputType() != null) { 
                t.add(name + ".in");
            }
        } else { 
            
            for (int i=0;i<childeren.length;i++) {
                Info head = childeren[i].get(0); 
                t.addAll(head.inQueueNames());
            }
        }

        return t;
    }
    
    public LinkedList<String> outQueueNames() { 
        
        LinkedList<String> t = new LinkedList<String>();
        
        if (childeren == null) { 
            if (stage.getComponent().getOutputType() != null) {
                t.add(name + ".out");
            }
        } else {                 
            for (int i=0;i<childeren.length;i++) {
                Info tail = childeren[i].get(childeren[i].size()-1); 
                t.addAll(tail.outQueueNames());
            }
        }

        return t;
    }                
    
    @Override
    public String toString() {

        if (childeren == null) { 
            return name;
        } else { 
            
            String result = "[";
            
            for (int i=0;i<childeren.length;i++) {
                
                 result += childeren[i].toString();
                 
                 if (i != childeren.length-1) { 
                     result += ", ";
                 }
            }
            
            return result + "]";
        }
    }

    public String getInputQueueName() {
        return name + ".in";
    }

    public String getOutputQueueName() {
        return name + ".out";
    }
}
