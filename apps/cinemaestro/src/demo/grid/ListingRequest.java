package demo.grid;

import ibis.ipl.IbisIdentifier;

import java.io.FileNotFoundException;

import util.FileFinder;
import util.FileInfo;
import util.FilesetDescription;

public class ListingRequest extends Request {

    /** 
     * Generated
     */
    private static final long serialVersionUID = 5216766657926047554L;

    private final FilesetDescription description;    
    
    public ListingRequest(FilesetDescription description) {
        this.description = description;
    }
    
    @Override
    public Object execute(IbisIdentifier local) { 
        try {            
            
            System.out.println("[*] Executing listing request: "+ description);
            
            FileInfo [] files = FileFinder.find(description); 
            
            if (files == null && files.length == 0) { 
                System.out.println("[*] No files found!");
                     
                return new ListingReply(local, -1,-1,-1);
            }

            // Two cases allowed  here, we either have a continuous set of files
            // (e.g, if we have a shared filesystem), or we have a strided set. 
            long stride = -1;
            
            if (FileInfo.isContinuous(files)) {
                stride = 1; 
            } else if (FileInfo.isStrided(files)) {
                stride = FileInfo.getStride(files);
            } else {
                // No (legal) fileset found
                System.out.println("[*] No legal files found!");
                
                return new ListingReply(local, -1,-1,-1);
            }
                
            long start = FileInfo.getStart(files);
            long end = FileInfo.getEnd(files);
                
            return new ListingReply(local, start, end, stride);            
        } catch (FileNotFoundException e) {
            System.out.println("[*] Got excetion " + e);
            e.printStackTrace();
            
            return new ListingReply(local, -1,-1,-1);
        }
    }
    
}
