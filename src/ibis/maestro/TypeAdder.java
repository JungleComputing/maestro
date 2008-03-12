package ibis.maestro;

/**
 * The interface of a type adder class.
 * This class adds supported types to a worker based on the
 * supported types of its known neighbors.
 * 
 * @author Kees van Reeuwijk
 *
 */
public interface TypeAdder {
    /**
     * Register with this worker that a neighbor supports the given type.
     * That may allow it to support a new type, since it knows what to
     * do with resulting jobs.
     * 
     * @param w The worker to update the types for.
     * @param t The type that a neighbor supports.
     */
    void registerNeighborType( Worker w, JobType t );
}