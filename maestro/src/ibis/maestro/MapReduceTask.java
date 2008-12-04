package ibis.maestro;

/**
 * The interface of a map/reduce task in the Maestro workflow system.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public interface MapReduceTask extends Task {
    /**
     * Given an input, submits a number of tasks to the given handler.
     * 
     * @param input
     *            The input value of this map.
     * @param handler
     *            The handler.
     */
    void map(Object input, MapReduceHandler handler);

    /**
     * Reports back a result.
     * 
     * @param id
     *            The identifier of the result.
     * @param result
     *            The result.
     */
    void reduce(Object id, Object result);

    /**
     * Returns the final result of the reduction. This method is invoked after
     * all jobs submitted by the map/reduce task have been reduced.
     * 
     * @return The result.
     */
    Object getResult();
}
