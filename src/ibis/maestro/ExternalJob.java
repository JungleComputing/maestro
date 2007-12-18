package ibis.maestro;

import java.io.File;
import java.io.IOException;

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
    
    /** Given a command string, construct a script to execute it.
     * 
     * @param command The command to execute.
     * @return The shell script to execute the command.
     */
    private static String buildScript( String command )
    {
	// FIXME: handle redirection.
	return "#!/bin/sh\n" + command + "\n";
    }

    /**
     * Given a bunch of bytes that represent a script, and a list of jobs to run, construct
     * a sandbox, fill it with a script file, and execute it with the given jobs as parameters. 
     * @param script The script text to executed.
     * @param jobnames The jobs to to run in the script.
     * @return Whether the script was executed sucesssfully.
     */
    public boolean run()
    {
        try {
            if( traceCommands ) {
                System.out.print( "Running job " + this );
            }
            // FIXME: more robust sandbox creation.
            File sandbox = new File( "/tmp/sandbox-" + label++ );
            File scriptFile = new File( sandbox, "script" );
            sandbox.mkdir();
            for( FileContents c: inputFiles ) {
        	c.create( sandbox );
            }
            String script = buildScript(command);
            writeFile( scriptFile, script );
            RunProcess p = new RunProcess( new String[] { "chmod", "+x", scriptFile.getAbsolutePath() }, null );
            int exitcode = p.getExitStatus();
            if( exitcode != 0 ) {
                System.err.println( "chmod execution returned exit code " + exitcode );
                byte e[] = p.getStderr();
                System.err.write( e );
                return false;                
            }
            String cmd[] = new String[jobnames.length+1];
            cmd[0] = scriptFile.getAbsolutePath();
            for( int i = 0; i < jobnames.length; i++ ) {
                cmd[i+1] = jobnames[i];
            }
            p = new RunProcess( cmd, null, sandbox );
            exitcode = p.getExitStatus();
            if( traceCommands ) {
                System.out.println( " done" );
            }
            if( exitcode != 0 ){
                if( traceCommands ){
                    System.out.println( "Script execution returned exit code " + exitcode );
                }
                byte e[] = p.getStderr();
                System.err.write( e );
                byte o[] = p.getStdout();
                System.err.write( o );
                return false;
            }
            removeSandbox( sandbox );
            return true;
        }
        catch (IOException e) {
            if( traceCommands ) {
                System.out.println( "Script execution failed: " + e );
                e.printStackTrace();
            }
            return false;
        }
    }

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

