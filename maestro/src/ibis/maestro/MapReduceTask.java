package ibis.maestro;

/**
 * The interface of a map/reduce task in the Maestro master/worker system.
 * @author Kees van Reeuwijk
 *
 */
public interface MapReduceTask extends Task {
    /**
     * Given an input, submits a number of tasks to the handler.
     *
     * @param input The input value of this map.
     * @param node The handler.
     */
    void map( Object input, MapReduceHandler handler );
}
