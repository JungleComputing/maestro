package processors;

import image.Image;
import image.ImageQueue;
import util.config.ComponentDescription;

public class Discarder<I extends Image> extends ImageConsumer<I> {

    @SuppressWarnings("unchecked")
    public Discarder(int componentNumber, String name, ImageQueue in, StatisticsCallback publisher) {
        super(componentNumber, "Discarder", name, in, publisher);
    }

    @Override
    public void process() throws Exception {
        
        Image i = in.get();

        int next = 0;
        
        while (i != null) {
        
            System.out.println("[*] Got image " + i.number + " expected " + next++);
            
            processedImage(i.number, 0, i.getSize(), 0, in.getMayBlock());
            i = in.get();
        }
    }
    
    public static Class<? extends Image> getInputQueueType() {
        return Image.class;
    }
    
    public static Class<? extends Image> getOutputQueueType() {
        return null;
    }
    
    public static Discarder create(ComponentDescription c,ImageQueue<? extends Image> in, ImageQueue out, 
            StatisticsCallback publisher) 
        throws Exception {
   
        return new Discarder(c.getNumber(), c.getName(), in, publisher);
    }

}
