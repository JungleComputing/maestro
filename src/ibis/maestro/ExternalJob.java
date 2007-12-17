package ibis.maestro;

/**
 * The information necessary to run an external job:
 * a list of input files with their content, a list of output files
 * to capture, and a command to execute.
 * @author Kees van Reeuwijk
 *
 */
public class ExternalJob {
    private FileContents inputFiles[];
    private String outputFiles[];
    private String command;
}

