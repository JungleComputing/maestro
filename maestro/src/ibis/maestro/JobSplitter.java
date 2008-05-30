package ibis.maestro;

/**
 * Given an input object, constructs a sequence of jobs, and submits them
 * to the given job sink.
 *  
 * @author Kees van Reeuwijk
 *
 */
public interface JobSplitter {
    /**
     * 
     * @param in The input parameter for this splitter.
     * @param sink The job sink.
     */
    void split( Object in, Context context, TaskInput task );
}
