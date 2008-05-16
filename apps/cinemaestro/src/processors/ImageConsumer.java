package processors;

import image.Image;
import image.ImageQueue;

public abstract class ImageConsumer<I extends Image> extends ProcessorThread {

    protected final ImageQueue<I> in;
    
    /**
     * Construct a new ImageConsumer
     * 
     * @param logger
     * @param type
     * @param name
     */
    public ImageConsumer(final int componentNumber, final String type, 
            final String name, final ImageQueue<I> in, 
            StatisticsCallback publisher) {
        
        super(componentNumber, type, name, publisher);
        this.in = in;
    }
}
