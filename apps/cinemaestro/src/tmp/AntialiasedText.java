package tmp;

/*
 * Copyright (c) 1995 - 2008 Sun Microsystems, Inc.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 *   - Neither the name of Sun Microsystems nor the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */ 


import java.applet.*;
import java.awt.*;
import java.awt.event.*;
import java.awt.font.*;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;

/**
 * This class demonstrates how to use Rendering Hints to antialias text.
 */
public class AntialiasedText extends Applet {
          
    
    
    public void paint(Graphics g) {

        Graphics2D g2d = (Graphics2D)g;
/*
        
        
        String text = "10";
        Font font = new Font(Font.SERIF, Font.PLAIN, 100);
        g2d.setFont(font);
        
        // get metrics from the graphics
        FontMetrics metrics = g2d.getFontMetrics(font);
        // get the height of a line of text in this font and render context
        int hgt = metrics.getHeight();
        // get the advance of my text in this font and render context
        int adv = metrics.stringWidth(text);
        // calculate the size of a box to hold the text with some padding.
        Dimension size = new Dimension(adv+2, hgt+2);
        
        g2d.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING,
                             RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
        g2d.drawString(text, (300-(adv+2))/2, 150);
  */
        
        BufferedImage buffi = new BufferedImage(100, 100, BufferedImage.TYPE_INT_RGB);
        Graphics2D buffig = buffi.createGraphics();
        
        /*
        buffig.setColor(Color.blue);
        buffig.fillRect(0, 0, 15, 15);
        buffig.setColor(Color.lightGray);
        buffig.translate((15/2)-(5/2), (15/2)-(5/2));
        buffig.fillOval(0, 0, 7, 7); 
        /*
        Rectangle r = new Rectangle(0,0,25,25);
        g2.setPaint(new TexturePaint(buffi, r));
        */
        
        buffig.setBackground(Color.BLACK);
        buffig.setColor(Color.WHITE);
        
        TextLayout textTl = new TextLayout("1", new Font("Times", 1, 96), new FontRenderContext(null, false, false));
        AffineTransform textAt = new AffineTransform();
        textAt.translate(0, (float)textTl.getBounds().getHeight());
        Shape s = textTl.getOutline(textAt);
        
        Rectangle r = s.getBounds();

        // Sets the selected Shape to the center of the Canvas.
        AffineTransform toCenterAt = new AffineTransform();
        toCenterAt.concatenate(textAt);
        toCenterAt.translate(50, 50);

        buffig.transform(toCenterAt);
        buffig.fill(s);
        
        g2d.setPaint(new TexturePaint(buffi, new Rectangle(100, 100)));
    
    }

    public static void main(String[] args) {

        Frame f = new Frame("Antialiased Text Sample");
            
        f.addWindowListener(new WindowAdapter(){
                public void windowClosing(WindowEvent e) {
                    System.exit(0);
                }
            });

        f.add(new AntialiasedText());
        f.setSize(new Dimension(300, 300));
        f.setVisible(true);
    }
}
