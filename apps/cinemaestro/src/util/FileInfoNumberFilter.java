package util;

import java.io.File;
import java.io.FileFilter;
import java.util.Collections;
import java.util.LinkedList;

public class FileInfoNumberFilter implements FileFilter {

    private LinkedList<FileInfo> list = new LinkedList<FileInfo>();
    
    protected final String prefix;
    protected final String postfix;

    protected final int length;     
    
    protected final int positions;     
    protected final int size; 
    protected final int rank; 
    
    /**
     * Construct a new FileInfoNumberFilter
     * 
     * @param prefix
     * @param postfix
     * @param size
     * @param rank
     */
    public FileInfoNumberFilter(FilesetDescription desc, int rank, int size) {
        
        this.prefix = desc.getPrefix();
        this.postfix = desc.getPostfix();
        this.positions = desc.getPositions();
        
        this.size = size;
        this.rank = rank;
        
        this.length = (prefix.length() + postfix.length() + positions);
    }

    protected long getNumber(String name) { 

        if (!name.startsWith(prefix)) { 
            return -1;
        }
        
        if (!name.endsWith(postfix)) { 
            return -1;
        }

        if (name.length() != length) { 
            return -1;
        }
        
        int start = prefix.length();
        int end = name.length() - postfix.length();
        
        if (end <= start) { 
            return -1;
        }
        
        String tmp = name.substring(start, end);

        return Integer.parseInt(tmp);
    }
    
    public FileInfo [] getAcceptedFiles() { 
        
        Collections.sort(list);
        
        return list.toArray(new FileInfo[list.size()]);
    }
    
    public boolean accept(File f) {
        
        long value = getNumber(f.getName());
        
        if (value == -1) { 
            return false;
        }
        
        boolean accept = false; 
        
        if (size <= 0) { 
            accept = true;
        } else { 
            accept = (value % size) == rank;
        }
            
        if (accept) { 
            list.addLast(new FileInfo(f, value));
        }
        
        return accept;
    }

}
