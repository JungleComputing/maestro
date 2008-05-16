package processors;

import image.Image;
import image.ImageQueue;

public abstract class ImageProcessor<I extends Image, O extends Image> extends ProcessorThread {

    protected final ImageQueue<I> in;
    protected final ImageQueue<O> out;
    
    /**
     * Construct a new ImageProcessor
     * 
     * @param logger
     * @param type
     * @param name
     */
    public ImageProcessor(final int componentNumber, final String type, 
            final String name, final ImageQueue<I> in, final ImageQueue<O> out, 
            StatisticsCallback publisher) {
        
        super(componentNumber, type, name, publisher);
        this.in = in;
        this.out = out;
    }
        
}
