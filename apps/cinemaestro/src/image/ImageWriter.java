package image;

public abstract class ImageWriter<T extends Image> extends Thread {
    
    private final ImageQueue<T> in;

    public ImageWriter(ImageQueue<T> in) {
        this.in = in;
    }
    
    public abstract boolean init();
    public abstract boolean writeImage(T image);
    public abstract void close();
    
    public void run() {

        boolean cont = init();
    
        while (cont) { 
            T image = in.get();
            
            if (image != null) { 
                cont = writeImage(image);
            } else { 
                cont = false;
            }
        }
    
        close();
    }
}
