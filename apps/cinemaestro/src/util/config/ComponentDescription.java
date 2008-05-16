package util.config;

import java.io.Serializable;
import java.util.HashMap;

import util.FilesetDescription;

public class ComponentDescription implements Serializable {

    /** 
     * Generated
     */
    private static final long serialVersionUID = 1159250447723491824L;
  
    private final int number;
    
    private final int stage;
    private final int size; 
    private final int rank; 
    
    private final String getName;
    private final String putName;
    
    private final QueueDescription getQ;
    private final QueueDescription putQ;
    
    private final HashMap<String, FilesetDescription> filesets;
  
    private final Component component;
    
    public ComponentDescription(int number, Component component,  
           // String input, String output, 
            int stage, int rank, int size, 
            String getName, String putName, 
            QueueDescription getQ, final QueueDescription putQ, 
            HashMap<String, FilesetDescription> filesets) {
        
        this.number = number;
        this.component = component;
     
        this.getQ = getQ;
        this.putQ = putQ;
        
        this.getName = getName;
        this.putName = putName;
        
        this.filesets = filesets;
        
        this.stage = stage;
        this.rank = rank;
        this.size = size;
    }
    
    public Component getComponent() { 
        return component;
    } 
    
    public Class getClazz() {
        return component.getClazz();
    }

    public String getInput() {
        return getName;
    }

    public String getOutput() {
        return putName;
    }

    public String getName() {
        return component.getName();
    }

    public HashMap<String, String> getOptions() {
        return component.getOptions();
    }

    
    public QueueDescription getGetQ() {
        return getQ;
    }

    public QueueDescription getPutQ() {
        return putQ;
    }

    public int getStage() {
        return stage;
    }
    
    public int getRank() {
        return rank;
    }
    
    public int getSize() {
        return size;
    }

    public int getNumber() {
        return number;
    }
    
    public FilesetDescription getFileSet() {
    
        // TODO: cross our fingers that there is only one!
        if (filesets == null) { 
            return null;
        }
        
        if (filesets.size() == 0) { 
            return null;
        }
        
        if (filesets.size() > 1) {
            System.out.println("EEP: got more than one fileset!!!");
            return null;
        }
        
        return filesets.values().iterator().next();
    }
    
    public FilesetDescription getFileSet(String name) {
        
        // TODO: cross our fingers that there is only one!
        if (filesets == null) { 
            return null;
        }
        
        if (filesets.size() == 0) { 
            return null;
        }
        
        return filesets.get(name);
    }

    public String toString() { 
       
        String input = getInput();
        String output = getOutput();
        
        String tmp = number + " " + component.getName();
        
        if (input == null) { 
            tmp += " [in: none]";
        } else { 
            tmp += " [in: " + input + "]";
        }
        
        if (output == null) { 
            tmp += " [out: none]";
        } else { 
            tmp += " [out: " + output + "]";
        }
        
        return tmp;
    }
    

}
