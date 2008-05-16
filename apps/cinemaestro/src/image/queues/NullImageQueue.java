package image.queues;

import image.Image;
import image.ImageQueue;

public class NullImageQueue<T extends Image> extends ImageQueue<T> {
    
    public NullImageQueue(String name, int maxImages) { 
        super(name, maxImages);
    }
    
    public NullImageQueue(int maxImages) { 
        this("Anonymous", maxImages);
    }
    
    public T get() {
        logger.info("Get always returns null");
        return null;
    }

    public void put(T b) {
        // drop image!
        logger.info("Put always drops image");
    }

    @Override
    public boolean getMayBlock() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean putMayBlock() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getSize() {
        return 0;
    }

    @Override
    public void printStats() {
        // TODO Auto-generated method stub
        
    }
}

