package ibis.maestro;

import java.io.File;
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

    public void create( File dir ) throws IOException
    {
        File f = new File( dir, name );
        
        // FIXME: enable again.
        //Service.writeFile(f, contents);
    }
}
