package ibis.maestro;

/**
 * Local information about a node.
 * 
 * @author Kees van Reeuwijk.
 */
class LocalNodeInfo {
    final boolean suspect;

    /** The number of currently running tasks of each type. */
    private final int currentTasks[];

    /** The transmission time to the node for each type. */
    private final double transmissionTime[];

    /**
     * The predicted turnaround time to the master for each type of task. Used
     * for scheduling deadlines.
     */
    private final double predictedDuration[];

    LocalNodeInfo(boolean suspect, int[] currentTasks, double[] transmissionTime,
            double predictedDuration[]) {
        this.suspect = suspect;
        this.currentTasks = currentTasks;
        this.transmissionTime = transmissionTime;
        this.predictedDuration = predictedDuration;
    }

    int getCurrentTasks(TaskType type) {
        return currentTasks[type.index];
    }

    double getTransmissionTime(TaskType type) {
        return transmissionTime[type.index];
    }

    double getTransmissionTime(int ix) {
        return transmissionTime[ix];
    }

    /**
     * Given a task type, returns the predicted duration of the task on the
     * local node. (Used for scheduling deadlines.)
     * 
     * @param type
     *            The type of task we want the prediction for.
     * @return The predicted duration of the task in seconds.
     */
    double getPredictedDuration(TaskType type) {
        return predictedDuration[type.index];
    }
}
