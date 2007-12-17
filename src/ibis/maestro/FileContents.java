package ibis.maestro;

import java.io.File;

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

    public void create( File dir )
    {
	// FIXME: implement this
    }
}
