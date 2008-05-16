package util;

import java.io.File;

public class FileInfo implements Comparable<FileInfo> {

    public final File file;
    public final long number;
    
    public FileInfo(File file, long number) { 
        this.file = file;
        this.number = number;
    }

    public int compareTo(FileInfo other) {
        return (int)(number - other.number);
    }
    
    public String toString() { 
        return file + ":" + number;
    }
    
    public static boolean isStrided(FileInfo [] f) { 
        
        // Note: assumes f is sorted in ascending/descending order of numbers
        if (f == null || f.length == 0) { 
            return false;
        }
        
        if (f.length == 1) { 
            return true;
        }
        
        long len = f[1].number - f[0].number;
        
        for (int i=2;i<f.length;i++) { 
            
            long tmp = f[i].number - f[i-1].number;
            
            if (tmp != len) { 
                return false;
            }
        }
     
        return true;
    }

    public static boolean isContinuous(FileInfo [] f) { 
        
        // Note: assumes f is sorted in ascending/descending order of numbers
        if (f == null || f.length == 0) { 
            return false;
        }
        
        if (f.length == 1) { 
            return true;
        }
        
        for (int i=1;i<f.length;i++) { 
            
            long tmp = f[i].number - f[i-1].number;
            
            if (tmp != 1) { 
                return false;
            }
        }
     
        return true;
    }

    public static int getStride(FileInfo [] f) { 
       
        // Note: assumes f is sorted in ascending/descending order of numbers
        if (f == null || f.length == 0) { 
            return 0;
        }
        
        if (f.length == 1) { 
            return 0;
        }
        
        return (int) (f[1].number - f[0].number);
    }   
    
    public static long getStart(FileInfo [] f) { 
        
        // Note: assumes f is sorted in ascending/descending order of numbers
        if (f == null || f.length == 0) { 
            return -1;
        }
        
        return f[0].number;
    }
    
    public static long getEnd(FileInfo [] f) { 
        
        // Note: assumes f is sorted in ascending/descending order of numbers
        if (f == null || f.length == 0) { 
            return -1;
        }
        
        return f[f.length-1].number;
    }
}
