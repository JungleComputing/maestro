package processors;

import image.Image;
import image.ImageQueue;
import util.config.ComponentDescription;

public class QueueCombiner extends ImageProcessor<Image, Image> {
    
    public QueueCombiner(int componentNumber, String name, ImageQueue<Image> in, 
            ImageQueue<Image> out,
            StatisticsCallback publisher) {

        // Store the name and in/output queues
        super(componentNumber, "Combiner", name, in, out, publisher);
    }
    
    @Override
    public void process() { 

        Image i = in.get();

        while (i != null) {
            processedImage(i.number, 0, i.getSize(), i.getSize(), out.putMayBlock());
            out.put(i);
            i = in.get();
        }

        out.setDone();
    }
    
    public static Class<Image> getInputQueueType() {
        return Image.class;
    }

    public static Class<Image> getOutputQueueType() {
        return Image.class;
    }    
    
    public static QueueCombiner create(ComponentDescription c,
            ImageQueue<Image> in, ImageQueue<Image> out, 
            StatisticsCallback publisher) 
        throws Exception {
        
        return new QueueCombiner(c.getNumber(), c.getName(), in, out, publisher);
    }
    
    
}
