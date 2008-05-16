package util;

import java.util.HashMap;

public class Options {

    public static int getIntOption(HashMap<String, String> options, 
            String name, boolean required, int defaultValue) throws Exception { 
        
        String value = options.get(name);
        
        if (value == null) { 
            if (required) { 
                throw new Exception("Option " + name + " not found!");
            } else { 
                return defaultValue;
            }
        }
        
        return Integer.parseInt(value);
    }
    
    public static double getDoubleOption(HashMap<String, String> options, 
            String name, boolean required, double defaultValue) throws Exception { 
        
        String value = options.get(name);
        
        if (value == null) { 
            if (required) { 
                throw new Exception("Option " + name + " not found!");
            } else { 
                return defaultValue;
            }
        }
        
        return Double.parseDouble(value);
    }
    
    
    public static boolean getBooleanOption(HashMap<String, String> options, 
            String name, boolean required, boolean defaultValue) throws Exception { 
        
        String value = options.get(name);
        
        if (value == null) { 
            if (required) { 
                throw new Exception("Option " + name + " not found!");
            } else { 
                return defaultValue;
            }
        }
        
        return Boolean.parseBoolean(value);
    }
    
    public static String getStringOption(HashMap<String, String> options, 
            String name, boolean required, String defaultValue) throws Exception { 
        
        String value = options.get(name);
        
        if (value == null) { 
            if (required) { 
                throw new Exception("Option " + name + " not found!");
            } else { 
                return defaultValue;
            }
        }
        
        return value;
    }
    
    public static int getIntOption(HashMap<String, String> options, 
            String name) throws Exception { 
        return getIntOption(options, name, true, -1);
    }

    public static double getDoubleOption(HashMap<String, String> options, 
            String name) throws Exception { 
        return getDoubleOption(options, name, true, -1);
    }
    
    public static boolean getBooleanOption(HashMap<String, String> options, 
            String name) throws Exception { 
        return getBooleanOption(options, name, true, false); 
    }
    
    public static String getStringOption(HashMap<String, String> options, 
            String name) throws Exception { 
        return getStringOption(options, name, true, null);
    }
    
    
}
