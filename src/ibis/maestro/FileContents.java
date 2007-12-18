package ibis.maestro;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author Kees van Reeuwijk
 *
 * The name of a file with its contents.
 */
public class FileContents {
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
        
        f.delete();  // First make sure it doesn't exist.
        FileWriter output = new FileWriter( f );
        output.write( contents );
        output.close();
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
