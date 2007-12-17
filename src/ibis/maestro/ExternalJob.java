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
    
    /** Constructs a new job.
     * 
     * @param inputFiles The files that should be present before the job is run.
     * @param outputFiles The files to get after the job has finished.
     * @param command The command to execute.
     */
    public ExternalJob(FileContents[] inputFiles, String[] outputFiles, String command) {
        super();
        this.inputFiles = inputFiles;
        this.outputFiles = outputFiles;
        this.command = command;
    }
}

