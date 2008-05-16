package image.queues;

import image.Image;

public class GetOnlyQueue<T extends Image> extends SimpleImageQueue<T> {
    
    protected GetOnlyQueue(String name, int maxImages) { 
        super(name, maxImages);
    }
    
    protected GetOnlyQueue(int maxImages) { 
        this("Anonymous", maxImages);
    }
    
    @Override
    public void put(T b) {
        throw new RuntimeException("Put not allowed!");
    }

    // This forwards the put to the super implemetation. Can only be used by 
    // subclasses or package members!
    protected void secretPut(T d) {
        super.put(d);
    }
    
    @Override
    public boolean putMayBlock() {
        return false;
    }     
}

