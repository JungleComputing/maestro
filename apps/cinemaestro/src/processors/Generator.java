package processors;

import image.Convert;
import image.ImageQueue;
import image.RGB24Image;
import image.RGB48Image;
import image.RGBImage;
import image.UncompressedImage;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.Shape;
import java.awt.font.FontRenderContext;
import java.awt.font.TextAttribute;
import java.awt.font.TextLayout;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.text.AttributedCharacterIterator;
import java.text.AttributedString;
import java.util.HashMap;

import util.Options;
import util.config.ComponentDescription;

public class Generator extends ImageProducer<UncompressedImage> {
    
    private int w;
    private int h;
    private int images;
    private int bits;
    private int rank;
    private int size;
    
    private long frameGap = 0;
    
    public Generator(int componentNumber, String name, 
            ImageQueue<UncompressedImage> out, int w, int h, int images, 
            int bits, int rank, int size, int fps, StatisticsCallback publisher) {
        
        super(componentNumber, "UncompressedFileReader", name, out, publisher);
    
        this.w = w;
        this.h = h;
        this.bits = bits;
        this.images = images;
        this.rank = rank;
        this.size = size;
        
        if (fps > 0) { 
            frameGap = (1000*size) / fps; 
        }        
    }
   
    private void drawDemo(int w, int h, int number, Graphics2D g2) {

        FontRenderContext frc = g2.getFontRenderContext();
        Font f = new Font("sansserif",Font.PLAIN,w/10);
        Font f1 = new Font("sansserif",Font.ITALIC,w/10);
        String s = "Generated Image";
        AttributedString as = new AttributedString(s);
   
        /*
         * applies the TextAttribute.Font attribute to the AttributedString 
         * with the range 0 to 10, which encompasses the letters 'A' through
         * 'd' of the String "AttributedString"
         */ 
        as.addAttribute(TextAttribute.FONT, f, 0, 10 );

        /*
         * applies the TextAttribute.Font attribute to the AttributedString 
         * with the range 10 to the length of the String s, which encompasses
         * the letters 'S' through 'g' of String "AttributedString"
         */ 
        as.addAttribute(TextAttribute.FONT, f1, 10, s.length() );

        AttributedCharacterIterator aci = as.getIterator();

        // creates a TextLayout from the AttributedCharacterIterator
        TextLayout tl = new TextLayout (aci, frc);
        float sw = (float) tl.getBounds().getWidth();
        float sh = (float) tl.getBounds().getHeight();

        /*
         * creates an outline shape from the TextLayout and centers it
         * with respect to the width of the surface
         */
        Shape sha = tl.getOutline(AffineTransform.getTranslateInstance(w/2-sw/2, h*0.2+sh/2));
        g2.setColor(Color.blue);
        g2.setStroke(new BasicStroke(1.5f));
        g2.draw(sha);
        g2.setColor(Color.magenta);
        g2.fill(sha);


        // creates a TextLayout from the String "Outline"
        f = new Font("serif", Font.BOLD,w/10);
        tl = new TextLayout("# " + number, f, frc);
        sw = (float) tl.getBounds().getWidth();
        sh = (float) tl.getBounds().getHeight();
        sha = tl.getOutline(AffineTransform.getTranslateInstance(w/2-sw/2,h*0.5+sh/2));
        g2.setColor(Color.black);
        g2.draw(sha);
        g2.setColor(Color.red);
        g2.fill(sha);


        f = new Font("Italic shear",Font.ITALIC,w/16);
 
        /*
         * creates a new shearing AffineTransform 
         */ 
        AffineTransform fontAT = new AffineTransform();
        fontAT.shear(-0.2, 0.0);

        // applies the fontAT transform to Font f
        Font derivedFont = f.deriveFont(fontAT);

        /*
         * creates a TextLayout from the String "Italic-Shear" and with
         * the transformed Font object
         */
        tl = new TextLayout("On: " + rank, derivedFont, frc);
        sw = (float) tl.getBounds().getWidth();
        sh = (float) tl.getBounds().getHeight();
        sha = tl.getOutline(AffineTransform.getTranslateInstance(w/2-sw/2,h*0.80f+sh/2));
        g2.setColor(Color.green);
        g2.draw(sha);
        g2.setColor(Color.black);
        g2.fill(sha);
        
    }

    private UncompressedImage generate(int number) throws Exception { 
            
        BufferedImage buf = new BufferedImage(w, h, BufferedImage.TYPE_3BYTE_BGR);
        Graphics2D g2 = buf.createGraphics();
        g2.setBackground(Color.WHITE);
        g2.setColor(Color.RED);
        g2.clearRect(0, 0, w, h);
        g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING,
                            RenderingHints.VALUE_ANTIALIAS_ON);
        
        drawDemo(w, h, number, g2);

        Raster r = buf.getRaster();
        
      /*  for (int i=0;i<h;i++) { 
            for (int j=0;j<w;j++) { 
                int [] tmp = r.getPixel(j, i, (int [])null);
                
                if (tmp[0] != 0) {
                    System.out.println("Found pixel!" + tmp[0]);
                }
                
            }
        }
        */
        
        int num = rank + number * size;
        
        RGBImage image = Convert.toRGBImage(num, null, buf.getRaster());
       
        if (bits == 24) { 
            if (image instanceof RGB24Image) { 
                return image;
            } else { 
                return Convert.toRGB24((RGB48Image) image);
            }
        } else if (bits == 48) { 
            if (image instanceof RGB48Image) { 
                return image;
            } else { 
                return Convert.toRGB48((RGB24Image) image);
            }
        } else { 
            throw new Exception("Unsupported image type!");
        }  
    }
        
    @Override
    public void process() {

        long last = 0;

        System.out.println("[*] GENERATOR TARGET GAP " + frameGap);
        
        for (int i=0;i<images;i++) { 
            try { 
                long t1 = System.currentTimeMillis();

                UncompressedImage c = generate(i);

                long t2 = System.currentTimeMillis();

                processedImage(c.number, t2-t1, c.getSize(), c.getSize(), 
                        out.putMayBlock());
                
                if (frameGap > 0) { 
                    if (last > 0) { 
                        // Lets produce the frames 3 ms. early, just to be sure
                        long sleep = frameGap - (System.currentTimeMillis() - last) - 3;
                        
                        if (sleep > 0) { 
                            Thread.sleep(sleep);
                        } else { 
                            System.out.println("[*] GENERATOR MISSED DEADLINE! " + sleep);
                        }
                    }

                    last = System.currentTimeMillis();
                } 
                
                

                out.put(c);
            } catch (Exception e) {
                System.out.println("[*] EEP: " + e);
            }
        }
        
        out.setDone();
    }

    public static Class getInputQueueType() { 
        return null;
    }
    
    public static Class<UncompressedImage> getOutputQueueType() {
        return UncompressedImage.class;
    }    
    
    public static Generator create(ComponentDescription c, 
            ImageQueue in,
    ImageQueue<UncompressedImage> out,
            StatisticsCallback publisher) throws Exception {
   
        HashMap<String, String> options = c.getOptions();
        
        int w = Options.getIntOption(options, "width");
        int h = Options.getIntOption(options, "height");
        int frames = Options.getIntOption(options, "frames");
        int bits = Options.getIntOption(options, "bits");
        
        int fps = Options.getIntOption(options, "fps", false, 0);
                       
        return new Generator(c.getNumber(), c.getName(), out, 
                w, h, frames, bits, c.getRank(), c.getSize(), fps, publisher);
    }
    
}
