package processors;

import image.Image;
import image.ImageQueue;

public abstract class ImageProducer<O extends Image> extends ProcessorThread {

    protected final ImageQueue<O> out;
    
    /**
     * Construct a new ImageConsumer
     * 
     * @param logger
     * @param type
     * @param name
     */
    public ImageProducer(final int componentNumber, final String type, final String name, 
            final ImageQueue<O> out, StatisticsCallback publisher) {
        
        super(componentNumber, type, name, publisher);
        this.out = out;
    }
        
}