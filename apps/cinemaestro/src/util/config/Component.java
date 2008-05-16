package util.config;

import image.ImageQueue;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashMap;

import processors.ProcessorThread;

class Component implements Serializable {
    private static final long serialVersionUID = 3539004796510858603L;

    private final String name;
    private final Class clazz;
    
    private final Class inputType;
    private final Class outputType;
    
    private final HashMap<String, String> options = new HashMap<String, String>();
    
    public Component(String name, Class clazz, Class inputType, Class outputType) { 
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
        
    public Class getClazz() {
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
   
    public Class getInputType() {
        return inputType;
    }

    public Class getOutputType() {
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
    
    @SuppressWarnings("unchecked")
    public ProcessorThread create(String name, ImageQueue in, ImageQueue out, 
            HashMap<String, String> options) throws Exception {
      
        try {
            Method m = clazz.getDeclaredMethod("create", 
                    new Class [] { String.class, ImageQueue.class, 
                    ImageQueue.class, HashMap.class } );
            
            return (ProcessorThread) m.invoke(null, 
                    new Object [] { name, in, out, options }); 
     
        } catch (Exception e) {
            throw new Exception("Failed to create processor!", e);
        }
    }
}