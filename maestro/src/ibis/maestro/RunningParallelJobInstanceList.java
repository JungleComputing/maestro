package ibis.maestro;


import java.util.ArrayList;

/**
 * A list of running parallel job instances.
 * 
 * @author Kees van Reeuwijk
 *
 */
class RunningParallelJobInstanceList {
    final ArrayList<ParallelJobInstance> l = new ArrayList<ParallelJobInstance>();

    synchronized ParallelJobId register(ParallelJobInstance i) {
        final int id = l.size();

        l.add(i);
        return new ParallelJobId( id );
    }

}
