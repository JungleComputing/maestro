package util;

import java.io.File;
import java.io.FileNotFoundException;

public class FileFinder {
    
    public static FileInfo [] find(FilesetDescription desc, int rank, int size) throws FileNotFoundException { 
        
        File dir = new File(desc.getDir());
        
        if (!(dir.isDirectory() && dir.canRead())) {
            throw new FileNotFoundException("Directory does not exist!");        
        } 
        
        FileInfoNumberFilter filter = new FileInfoNumberFilter(desc, rank, size);
        
        dir.listFiles(filter);
        
        return filter.getAcceptedFiles();
    }
    
    public static FileInfo [] find(FilesetDescription desc) throws FileNotFoundException { 
        return find(desc, -1, -1);
    }
    
    public static void main(String [] args) { 
        
        try {
            
            FilesetDescription set = new FilesetDescription(false, args[0], 
                    args[1], args[2], Integer.parseInt(args[3]));
            
            int rank = Integer.parseInt(args[4]);
            int size = Integer.parseInt(args[5]);
                        
            FileInfo [] tmp = find(set, rank, size);
            
            for (FileInfo f : tmp) { 
                System.out.println(f);
            }
        } catch (NumberFormatException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } 
        
        
    }
    
}
