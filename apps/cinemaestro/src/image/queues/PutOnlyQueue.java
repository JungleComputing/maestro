package image.queues;

import image.Image;

public class PutOnlyQueue<T extends Image> extends SimpleImageQueue<T> {
    
    protected PutOnlyQueue(String name, int maxImages) { 
        super(name, maxImages);
    }
    
    protected PutOnlyQueue(int maxImages) { 
        this("Anonymous", maxImages);
    }
    
    public T get() {
        throw new RuntimeException("Get not allowed!");
    }
  
    // This forward the get to the super implemetation. Can only be used by 
    // subclasses or package members!
    protected T secretGet() { 
        return super.get();
    }
    
    @Override
    public boolean getMayBlock() {
        return false;
    }
}

