package processors;

import image.Image;
import image.ImageQueue;
import util.Options;
import util.config.ComponentDescription;

public class FrameRateCounter extends ImageConsumer {

    private final int frames;
    
    private final long [] times;
    
    @SuppressWarnings("unchecked")
    public FrameRateCounter(int componentNumber, String name, int frames, 
            ImageQueue in, StatisticsCallback publisher) {
        super(componentNumber, "Counter", name, in, publisher);
   
        this.frames = frames;
        
        times = new long[frames+2];
    }

    @Override
    public void process() throws Exception {
        
        int index = 0;
        
        times[index++] = System.currentTimeMillis();
        
        Image i = in.get();

        times[index++] = System.currentTimeMillis();
        
        int next = 0;
        
        while (i != null) {
        
            if (next != i.number) {
                System.out.println("[*] Got image " + i.number + " expected " + next);
            }
            
            next++;
            
            processedImage(i.number, 0, i.getSize(), 0, in.getMayBlock());
            i = in.get();
            times[index++] = System.currentTimeMillis();   
        }
        
        System.out.println("[*] Frame arrival times: ");

        for (int t=1;t<times.length-1;t++) { 
            
            long time = times[t] - times[t-1];
            double fps = 1000.0/time;
            
            System.out.println("[*] " + t + " " + time + " " + fps);
        }
        
        long total = times[frames-1] - times[1];
 
        double avgTime = ((double) total) / (frames-1);
        
        double fps = 1000.0 / avgTime;
        
        System.out.println("[*] Overall gap: " + (total/frames-1) + " ms.");
        System.out.println("[*] Overall fps: " + fps);
        
    
    }
    
    public static Class getInputQueueType() {
        return Image.class;
    }
    
    public static Class getOutputQueueType() {
        return null;
    }
    
    public static FrameRateCounter create(ComponentDescription c,ImageQueue in, ImageQueue out, 
            StatisticsCallback publisher) 
        throws Exception {
   
        int frames = Options.getIntOption(c.getOptions(), "frames");
        
        return new FrameRateCounter(c.getNumber(), c.getName(), frames, in, publisher);
    }

}
