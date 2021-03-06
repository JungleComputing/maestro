package util.config;

import image.Image;
import image.ImageQueue;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashMap;

import processors.ProcessorThread;

public class Component implements Serializable {
    private static final long serialVersionUID = 3539004796510858603L;

    private final String name;
    private final Class<? extends Component> clazz;
    
    private final Class<? extends Image> inputType;
    private final Class<? extends Image> outputType;
    
    private final HashMap<String, String> options = new HashMap<String, String>();
    
    public Component(String name, Class<? extends Component> clazz, Class<? extends Image> inputType, Class<? extends Image> outputType) { 
        this.name = name;
        this.clazz = clazz;
        this.inputType = inputType;
        this.outputType = outputType;
    }
    
    public String getName() { 
        return name;
    }
    
    public void addOption(final String key, final String value) { 
        options.put(key, value);
    }
    
    public String getOption(final String key) { 
        return options.get(key);
    }
        
    public Class<? extends Component> getClazz() {
        return clazz;
    }

    public HashMap<String, String> getOptions() {
        return options;
    }
    
    public boolean hasInputQueue() {
        return (inputType != null);
    }

    public boolean hasOutputQueue() {
        return (outputType != null);
    }
   
    public Class<? extends Image> getInputType() {
        return inputType;
    }

    public Class<? extends Image> getOutputType() {
        return outputType;
    }
    
    public boolean usesFileset() { 
        
        if (options == null || options.size() == 0) { 
            return false;
        }
        
        for (String s : options.values()) { 
            
            if (s.startsWith("fileset://")) { 
                return true;
            }
        }
        
        return false;
        
    }

    public ProcessorThread create(String nm, ImageQueue in, ImageQueue out, 
            HashMap<String, String> options) throws Exception {
      
        try {
            Method m = clazz.getDeclaredMethod("create", 
                    new Class [] { String.class, ImageQueue.class, 
                    ImageQueue.class, HashMap.class } );
            
            return (ProcessorThread) m.invoke(null, 
                    new Object [] { nm, in, out, options }); 
     
        } catch (Exception e) {
            throw new Exception("Failed to create processor!", e);
        }
    }
}