package util;

import java.io.Serializable;

public class FilesetDescription implements Serializable {

    /** 
     * Generated
     */
    private static final long serialVersionUID = 4315753136551089523L;

    private final String dir;

    private final String prefix;

    private final String postfix;

    private final int positions;
    
    private final boolean check;

    public FilesetDescription(final boolean check, final String dir, 
            final String prefix, final String postfix, final int positions) {
        this.check = check;
        this.dir = dir;
        this.prefix = prefix;
        this.postfix = postfix;
        this.positions = positions;
    }

   // public FilesetDescription(final String dir, final String prefix,
   //         final String postfix, final int positions) {
   // }
    
    public boolean getCheck() { 
        return check;
    }
    
    public String getDir() {
        return dir;
    }

    public String getPostfix() {
        return postfix;
    }

    public String getPrefix() {
        return prefix;
    }

    public int getPositions() {
        return positions;
    }
    
    public String toString() { 
        return "FileSetDescription: " + dir + " / " + prefix + "%" + positions + postfix + " " + check;
    }
}
