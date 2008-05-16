package image;

public abstract class ImageReader<T extends Image> extends Thread {

    private final ImageQueue<T> out;
   
    public ImageReader(ImageQueue<T> out) {
        this.out = out;
    }

    public abstract boolean init();
    public abstract T readImage();
    public abstract void close();
    
    public void run() {
 
        boolean cont = init();
        
        while (cont) { 
            
            T image = readImage();
            
            if (image != null) { 
                out.put(image);
            } else { 
                cont = false;
            }
        }
        
        out.setDone();
        
        close();
    }
    
}
