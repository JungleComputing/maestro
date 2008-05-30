package ibis.maestro;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;

/**
 * The name of a file with its contents.
 *
 * @author Kees van Reeuwijk
 *
 */
public class FileContents implements Serializable {
    /** Contractual obligation. */
    private static final long serialVersionUID = -5082035301445931441L;
    private String name;
    private String contents;

    /**
     * @param name The name of the file.
     * @param contents The contents of the file.
     */
    public FileContents(String name, String contents) {
	this.name = name;
	this.contents = contents;
    }

    /** Creates this file in the given directory.
     * 
     * @param dir The directory where the file should be created.
     * @throws IOException Thrown if for some reason the file cannot be created.
     */
    public void create( File dir ) throws IOException
    {
        File f = new File( dir, name );

        if( f.exists() ){
            if( !f.delete() ){
                // First make sure it doesn't exist.
                System.err.println( "Cannot delete file '" + f + "'" );
            }
        }
        FileWriter output = null;
        try {
            output = new FileWriter( f );
            output.write( contents );
        }
        finally {
            if( output != null ){
                output.close();
            }
        }
    }

    /** Returns true iff this file has the given name.
     * 
     * @param nm The name to compare to.
     * @return True iff the file has the given name.
     */
    public boolean hasName(String nm) {
	return name.equals(nm);
    }
}
