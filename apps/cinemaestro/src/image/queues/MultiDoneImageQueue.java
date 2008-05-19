package image.queues;

import image.Image;

public class MultiDoneImageQueue<T extends Image> extends SimpleImageQueue<T> {

    private final int doneCount;
    private int dones;
    
    public MultiDoneImageQueue(int maxImages, int doneCount) {
        super(maxImages);
        this.doneCount = doneCount;
    }
    
    public MultiDoneImageQueue(String name, int maxImages, int doneCount) {
        super(name, maxImages);
        this.doneCount = doneCount;
    }
    
    @Override
    public synchronized void setDone() {
   
        dones++;
        
        if (dones == doneCount) { 
            super.setDone();
        }
    }

    public static <U extends Image> MultiDoneImageQueue<U> create(Class<U> type, String name, int max, int doneCount) {
        // Not sure if this works....
        return new MultiDoneImageQueue<U>(name, max, doneCount);
    }
    
}
