package ibis.maestro;

import ibis.util.RunProcess;

import java.io.File;
import java.io.IOException;
import java.util.Vector;


/**
 * The information necessary to run an external job:
 * a list of input files with their content, a list of output files
 * to capture, and a command to execute.
 * @author Kees van Reeuwijk
 *
 */
public class ExternalJob implements Job {
    /** Contractual obligation. */
    private static final long serialVersionUID = -1100488263983745631L;
    private FileContents inputFiles[];
    private String outputFiles[];
    private Vector<String> command;
    private static final boolean traceCommands = true;
    private static long label = 0L;
    private static final JobType jobType = new JobType( "ExternalJob" );
    private final TaskIdentifier id;

    private static void tryToRemoveFile( File f )
    {
        if( f.exists() ){
            if( !f.delete() ){
                System.err.println( "Cannot delete existing sandbox file '" + f + '\'' );
            }
         }        
    }

    /** Given a sandbox directory remove its contents and the
     * directory itself.
     *
     * @param f The sandbox directory.
     */
    private static void removeSandbox( File f )
    {
        File files[] = f.listFiles();

        for( File fi: files ) {
            tryToRemoveFile( fi );
        }
        tryToRemoveFile( f );
    }

    static class RunResult implements JobResultValue {
        /** Contractual obligation. */
        private static final long serialVersionUID = 881469549150557400L;
        private final int exitcode;
        private final byte out[];
        private final byte err[];

        /**
         * Constructs a new run result with the given information.
         * @param exitcode The exit code of the process
         * @param out The text from stdout of the process
         * @param err The text from stderr of the process
         */
        public RunResult(final int exitcode, final byte[] out, final byte[] err) {
            super();
            this.exitcode = exitcode;
            this.out = out;
            this.err = err;
        }

        /** Returns the error text of this run result.
         * @return The error text.
         */
        public byte[] getErr() {
            return err;
        }

        /** Returns the exit code of this run result.
         * @return the exit code.
         */
        public int getExitcode() {
            return exitcode;
        }

        /** Returns the output text of this run result.
         * @return The output text.
         */
        public byte[] getOut() {
            return out;
        }        
    }
    
    /**
     * Runs this job.
     * @param context The context of this run.
     */
    @Override
    public void run( JobContext context )
    {
        File sandbox;
        ProcessBuilder builder;

        if( traceCommands ) {
            System.out.print( "Running job " + this );
        }
        try {
            // FIXME: more robust sandbox creation.
            sandbox = new File( "/tmp/sandbox-" + label++ );
            if( !sandbox.mkdir() ){
                // FIXME: more robust error handling.
                System.err.println( "Cannot create sanbox directory '" + sandbox + '\'' );
                return;
            }
            for( FileContents c: inputFiles ) {
                c.create( sandbox );
            }
            builder = new ProcessBuilder( command );
            builder.directory(sandbox);

        }
        catch ( IOException x ){
            // FIXME: more robust error handling.
            x.printStackTrace();
            return;
        }
        String a[] = new String[command.size()];
        command.toArray(a);
        RunProcess p = new RunProcess( a, null, sandbox );
        byte[] o = p.getStdout();
        byte[] e = p.getStderr();
        int exitcode = p.getExitStatus();
        if( traceCommands ) {
            System.out.println( " done" );
        }
        if( exitcode != 0 ){
            if( traceCommands ){
                System.out.println( "Script execution returned exit code " + exitcode );
            }
        }
        removeSandbox( sandbox );
        JobResultValue result = new RunResult( exitcode, o, e );
        context.submit( new ReportResultJob( id, result ) );
    }

    /** Constructs a new job.
     * 
     * @param id The identifier of the overall task.
     * @param inputFiles The files that should be present before the job is run.
     * @param outputFiles The files to get after the job has finished.
     * @param command The command to execute.
     */
    public ExternalJob(TaskIdentifier id, FileContents[] inputFiles, String[] outputFiles, Vector<String> command) {
        super();
        this.id = id;
        this.inputFiles = inputFiles;
        this.outputFiles = outputFiles;
        this.command = command;
    }

    /**
     * @return The type of this job.
     */
    @Override
    public JobType getType() {
	return jobType;
    }

}

