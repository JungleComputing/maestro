
package processors;

import image.ImageQueue;
import image.RGB24Image;
import image.RGB48Image;
import image.UncompressedImage;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;

import util.FilesetDescription;
import util.Options;
import util.config.ComponentDescription;

public class UncompressedFileWriter extends ImageConsumer<UncompressedImage> {

    private final File dir;
    private final FilesetDescription out;
    
    public UncompressedFileWriter(int componentNumber, String name, 
            ImageQueue<UncompressedImage> in, FilesetDescription fs,
            StatisticsCallback publisher) {
        
        super(componentNumber, "UncompressedFileWriter", name, in, publisher);
 
        this.out = fs;
        this.dir = new File(out.getDir());
    
    }
    
    private String getName(long number) { 
        
        long tmp = 10;
        
        String name = out.getPrefix();
        
        for (int i=1;i<out.getPositions();i++) { 
            if (number < tmp) {
                name += "0";
            }
            
            tmp = tmp * 10;
        }
        
        return name + number + out.getPostfix();
    }
    
    private DataOutputStream openFile(long number) throws FileNotFoundException { 
    
        String name = getName(number);
        
        File file = new File(dir, name);        
        FileOutputStream fs = new FileOutputStream(file);
        return new DataOutputStream(new BufferedOutputStream(fs));
    }    
    
    private void write(RGB48Image image) throws IOException {
        
        DataOutputStream out = openFile(image.number);
        
        out.writeInt(48);
        out.writeInt(image.width);
        out.writeInt(image.height);
        
        short [] data = (short[]) image.getData();
        
        for (int i=0;i<data.length;i++) { 
            out.writeShort(data[i]);
        }
        
        out.close();
    }
    
    private void write(RGB24Image image) throws IOException { 

        DataOutputStream out = openFile(image.number);
        
        out.writeInt(24);
        out.writeInt(image.width);
        out.writeInt(image.height);
        out.write((byte []) image.getData());
        out.close();
    }
        
    public void process() { 

        UncompressedImage i = in.get();

        while (i != null) {

            logger.info("Got image " + i.number);

            long t1 = System.currentTimeMillis();

            try {
                
                if (i instanceof RGB48Image) { 
                    write((RGB48Image) i);
                } else if (i instanceof RGB24Image) {
                    write((RGB24Image) i);
                }
            } catch (Exception e) { 
                System.out.println("[*] ERROR (" + name 
                        + "): failed to write " + name);
                e.printStackTrace();
            }

            long t2 = System.currentTimeMillis();

            long len = i.getSize();

            processedImage(i.number, t2-t1, len, len, in.getMayBlock());

            i = in.get();
        }
    }

    public static Class getInputQueueType() {
        return UncompressedImage.class;
    }

    public static Class getOutputQueueType() {
        return null;
    }

    public static UncompressedFileWriter create(ComponentDescription c,
            ImageQueue<UncompressedImage> in, ImageQueue out, 
            StatisticsCallback publisher) throws Exception {

        HashMap<String, String> options = c.getOptions();

        String tmp = Options.getStringOption(options, "output");
        
        if (!tmp.startsWith("fileset://")) { 
            throw new Exception("Fileset required!");
        }

        FilesetDescription fs = c.getFileSet(tmp.substring(10));
        
        if (fs == null) { 
            throw new Exception("Fileset required!");
        }
        
        File directory = new File(fs.getDir());
        
        if (!directory.exists() || !directory.canWrite()) { 
            throw new Exception("Directory " + directory + " not accessible!");
        }

        return new UncompressedFileWriter(c.getNumber(), c.getName(), in, 
                fs, publisher);
    }
}
