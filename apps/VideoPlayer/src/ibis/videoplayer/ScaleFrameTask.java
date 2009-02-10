package ibis.videoplayer;

import ibis.maestro.AtomicJob;

/**
 * A task to scale down a frame.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class ScaleFrameTask implements AtomicJob {
    private static final long serialVersionUID = -3938044583266505212L;
    private final int factor;

    ScaleFrameTask(int factor) {
        this.factor = factor;
    }

    /**
     * Returns the name of this task.
     * 
     * @return The name.
     */
    @Override
    public String getName() {
        return "Scale frame";
    }

    /**
     * Scales the given input, an image, and returns a scaled version of the
     * image.
     * 
     * @param obj
     *            The input of this function.
     * @param node
     *            Some info of the node we're running this on.
     * @return The scaled image.
     */
    @Override
    public Object run(Object obj) {
        Image frame = (Image) obj;
        System.out.println("Scaling " + frame);
        return frame.scaleDown(factor);
    }

    /**
     * @return True, because this job can run anywhere.
     */
    @Override
    public boolean isSupported() {
        return true;
    }
}
