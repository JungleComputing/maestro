package ibis.maestro;

import java.util.Comparator;

/**
 * The interface of a type information class.
 * 
 * Classes that implement this interface keep track of the
 * priority and appropriateness of executing a type of task
 * on the given node.
 *
 * @author Kees van Reeuwijk
 *
 */
public interface TypeInformation extends Comparator<JobType> {
    /**
     * Register with this worker the initial types it should support.
     * @param w The worker to initialize.
     */
    void initialize( Node w );

    /**
     * Register with this worker that a neighbor supports the given type.
     * That may allow it to support a new type, since it knows what to
     * do with resulting jobs.
     * 
     * @param w The worker to update the types for.
     * @param t The type that a neighbor supports.
     */
    void registerNeighborType( Node w, JobType t );
}
