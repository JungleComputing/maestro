package ibis.maestro;

import ibis.util.RunProcess;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

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
    private List<String> command;
    private static final boolean traceCommands = true;
    private static long label = 0L;

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

    private static boolean containsName( FileContents l[], String name )
    {
	for( FileContents c: l ){
	    if( c.hasName( name ) ) {
		return true;
	    }
	}
	return false;
    }

    static class RunResult {
        private final int exitcode;
        private final byte out[];
        private final byte err[];

        public RunResult(final int exitcode, final byte[] out, final byte[] err) {
            super();
            this.exitcode = exitcode;
            this.out = out;
            this.err = err;
        }

        public byte[] getErr() {
            return err;
        }

        public int getExitcode() {
            return exitcode;
        }

        public byte[] getOut() {
            return out;
        }        
    }
    
    private RunResult run()
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
        Map<String,String> env = System.getenv();
        Set<String> keys = env.keySet();
        String l[] = new String[keys.size()];
        int ix = 0;
        for( String k: env.keySet() ){
            String v = env.get( k );
            l[ix++] = k + "=" + v;
        }
        String a[] = new String[command.size()];
        command.toArray(a);
        RunProcess p = new RunProcess( a, l );
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
    public ExternalJob(FileContents[] inputFiles, String[] outputFiles, List<String> command) {
        super();
        this.inputFiles = inputFiles;
        this.outputFiles = outputFiles;
        this.command = command;
    }
}

