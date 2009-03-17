package ibis.maestro;


import java.util.ArrayList;

/**
 * A list of running parallel job instances.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class RunningParallelJobInstanceList {
    ArrayList<ParallelJobInstance> l = new ArrayList<ParallelJobInstance>();

    public synchronized ParallelJobId register(ParallelJobInstance i) {
        final int id = l.size();

        l.add(i);
        return new ParallelJobId( id );
    }

}
