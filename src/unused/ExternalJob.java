package unused;

import ibis.maestro.Job;
import ibis.maestro.JobReturn;
import ibis.maestro.JobType;
import ibis.maestro.Master;
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

    /** Given a sandbox directory remove its contents and the
     * directory itself.
     *
     * @param f The sandbox directory.
     */
    private static void removeSandbox( File f )
    {
        File files[] = f.listFiles();

        for( File fi: files ) {
            fi.delete();
        }
        f.delete();
    }

    static class RunResult implements JobReturn {
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
     * @return The return value of this job.
     */
    @Override
    public JobReturn run( Master master )
    {
        File sandbox;
        ProcessBuilder builder;

        if( traceCommands ) {
            System.out.print( "Running job " + this );
        }
        try {
            // FIXME: more robust sandbox creation.
            sandbox = new File( "/tmp/sandbox-" + label++ );
            sandbox.mkdir();
            for( FileContents c: inputFiles ) {
                c.create( sandbox );
            }
            builder = new ProcessBuilder( command );
            builder.directory(sandbox);

        }
        catch ( IOException x ){
            // FIXME: more robust error handling.
            x.printStackTrace();
            return null;
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
        return new RunResult( exitcode, o, e );
    }

    /** Constructs a new job.
     * 
     * @param inputFiles The files that should be present before the job is run.
     * @param outputFiles The files to get after the job has finished.
     * @param command The command to execute.
     */
    public ExternalJob(FileContents[] inputFiles, String[] outputFiles, Vector<String> command) {
        super();
        this.inputFiles = inputFiles;
        this.outputFiles = outputFiles;
        this.command = command;
    }

    /**
     * Compares this job with the given other job.
     * Since there is no reason to order jobs in this queue, 0
     * is always returned.
     * @param other The other job to compare to.
     * @return The result of the comparison.
     */
    public int compareTo( Job other )
    {
        // There is no reason to impose a special ordering on these job.
        // TODO: add a priority number.
        return 0;
    }

    /**
     * @return The type of this job.
     */
    @Override
    public JobType getType() {
	return jobType;
    }

}

