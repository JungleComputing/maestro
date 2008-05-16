package image;

import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.DataBufferUShort;
import java.awt.image.PixelInterleavedSampleModel;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;

public class Convert {

    private static final void convertRGB48toYUV24(short [] data, 
            byte [] Y, byte [] U, byte [] V,  
            int index1, int index2, int indexA, int indexB, int indexC) { 
        
        /*
         * Y  = (0.257 * R) + (0.504 * G) + (0.098 * B) + 16
         * V =  (0.439 * R) - (0.368 * G) - (0.071 * B) + 128
         * U = -(0.148 * R) - (0.291 * G) + (0.439 * B) + 128
         * 
         */

        final int r1 = (data[index1+0] & 0xffff) / 255;
        final int g1 = (data[index1+1] & 0xffff) / 255;
        final int b1 = (data[index1+2] & 0xffff) / 255;
        
        final int r2 = (data[index1+3] & 0xffff) / 255;
        final int g2 = (data[index1+4] & 0xffff) / 255;
        final int b2 = (data[index1+5] & 0xffff) / 255;
        
        final int r3 = (data[index2+0] & 0xffff) / 255;
        final int g3 = (data[index2+1] & 0xffff) / 255;
        final int b3 = (data[index2+2] & 0xffff) / 255;
        
        final int r4 = (data[index2+3] & 0xffff) / 255;
        final int g4 = (data[index2+4] & 0xffff) / 255;
        final int b4 = (data[index2+5] & 0xffff) / 255;        
                
        final int y1 = 16 + (int)(0.257 * r1 + 0.504 * g1 + 0.098 * b1);
        final int y2 = 16 + (int)(0.257 * r2 + 0.504 * g2 + 0.098 * b2);                
        final int y3 = 16 + (int)(0.257 * r3 + 0.504 * g3 + 0.098 * b3);
        final int y4 = 16 + (int)(0.257 * r4 + 0.504 * g4 + 0.098 * b4);
        
        final int rAvg = (r1 + r2 + r3 + r4) / 4;
        final int gAvg = (g1 + g2 + g3 + g4) / 4;
        final int bAvg = (b1 + b2 + b3 + b4) / 4;                        
                
        final int v = 128 + (int)(0.439 * rAvg - 0.368 * gAvg - 0.071 * bAvg) ; 
        final int u = 128 + (int)(-0.148 * rAvg - 0.291 * gAvg + 0.439 * bAvg);
        
        Y[indexA]   = (byte) y1;
        Y[indexA+1] = (byte) y2;
        Y[indexB]   = (byte) y3;
        Y[indexB+1] = (byte) y4;
        
        U[indexC] = (byte) (u & 0xFF);
        V[indexC] = (byte) (v & 0xFF);
    }
    
    private static final void convertRGB24toYUV24(byte [] data, 
            byte [] Y, byte [] U, byte [] V,  
            int index1, int index2, int indexA, int indexB, int indexC) { 
        
        /*
         * Y  = (0.257 * R) + (0.504 * G) + (0.098 * B) + 16
         * V =  (0.439 * R) - (0.368 * G) - (0.071 * B) + 128
         * U = -(0.148 * R) - (0.291 * G) + (0.439 * B) + 128
         * 
         */

        final int r1 = (data[index1+0] & 0xff);
        final int g1 = (data[index1+1] & 0xff);
        final int b1 = (data[index1+2] & 0xff);
        
        final int r2 = (data[index1+3] & 0xff);
        final int g2 = (data[index1+4] & 0xff);
        final int b2 = (data[index1+5] & 0xff);
        
        final int r3 = (data[index2+0] & 0xff);
        final int g3 = (data[index2+1] & 0xff);
        final int b3 = (data[index2+2] & 0xff);
        
        final int r4 = (data[index2+3] & 0xff);
        final int g4 = (data[index2+4] & 0xff);
        final int b4 = (data[index2+5] & 0xff);        
                
        final int y1 = 16 + (int)(0.257 * r1 + 0.504 * g1 + 0.098 * b1);
        final int y2 = 16 + (int)(0.257 * r2 + 0.504 * g2 + 0.098 * b2);                
        final int y3 = 16 + (int)(0.257 * r3 + 0.504 * g3 + 0.098 * b3);
        final int y4 = 16 + (int)(0.257 * r4 + 0.504 * g4 + 0.098 * b4);
        
        final int rAvg = (r1 + r2 + r3 + r4) / 4;
        final int gAvg = (g1 + g2 + g3 + g4) / 4;
        final int bAvg = (b1 + b2 + b3 + b4) / 4;                        
                
        final int v = 128 + (int)(0.439 * rAvg - 0.368 * gAvg - 0.071 * bAvg) ; 
        final int u = 128 + (int)(-0.148 * rAvg - 0.291 * gAvg + 0.439 * bAvg);
       
        Y[indexA]   = (byte) (y1 & 0xFF);
        Y[indexA+1] = (byte) (y2 & 0xFF);
        Y[indexB]   = (byte) (y3 & 0XFF);
        Y[indexB+1] = (byte) (y4 & 0xFF);
        
        U[indexC] = (byte) (u & 0xFF);
        V[indexC] = (byte) (v & 0xFF);
    }

    public static final void RGB48toYUV24(short [] rgb, 
            byte [] Y, byte [] U, byte [] V, int width, int height) { 
        
        // This RGB to YUV convertion used a block of 2x2 pixels. It basically 
        // coverts and downsamples the UV components at the same time...
        int index = 0;
        
        for (int h=0;h<height-1;h+=2) { 
            for (int w=0;w<width-1;w+=2) {
                convertRGB48toYUV24(rgb, Y, U, V, 
                        h*(width*3)+(w*3), 
                        (h+1)*(width*3)+(w*3), 
                        h*width+w, 
                        (h+1)*width+w, 
                        index++);
            }
        }
    }
    
    public static final void RGB24toYUV24(byte [] rgb, 
            byte [] Y, byte [] U, byte [] V, int width, int height) { 
        
        // This RGB to YUV convertion used a block of 2x2 pixels. It basically 
        // coverts and downsamples the UV components at the same time...
        int index = 0;
        
        for (int h=0;h<height-1;h+=2) { 
            for (int w=0;w<width-1;w+=2) {
                convertRGB24toYUV24(rgb, Y, U, V, 
                        h*(width*3)+(w*3), 
                        (h+1)*(width*3)+(w*3), 
                        h*width+w, 
                        (h+1)*width+w, 
                        index++);
            }
        }
    }
    
    
    public static final void RGB64toRGB48(short [] rgbIn, short [] rgbOut) { 
        
        // This conversion simply discards the alpha channel of the source
       
        int indexSrc = 0;
        int indexDst = 0;
         
        while (indexSrc < rgbIn.length) { 
            
            rgbOut[indexDst++] = rgbIn[indexSrc++];
           
            // Skip every fourth value
            if (indexSrc % 4 == 3) { 
                indexSrc++;
            }
        }
    }
 
    public static final void RGB32toRGB24(byte [] rgbIn, byte [] rgbOut) { 
        
        // This conversion simply discards the alpha channel of the source
       
        int indexSrc = 0;
        int indexDst = 0;
         
        while (indexSrc < rgbIn.length) { 
            
            rgbOut[indexDst++] = rgbIn[indexSrc++];
           
            // Skip every fourth value
            if (indexSrc % 4 == 3) { 
                indexSrc++;
            }
        }
    }
    
    public static final void RGB24toRGB48(byte [] rgbIn, short [] rgbOut) { 
        
        int index = 0;
       
        while (index < rgbIn.length) { 
            rgbOut[index] = (short) ((rgbIn[index] & 0xFF) * 255); 
            index++;
        }
    }
    
    public static final void RGB48toRGB24(short [] rgbIn, byte [] rgbOut) { 
        
        int index = 0;
       
        while (index < rgbIn.length) { 
            rgbOut[index] = (byte) (((rgbIn[index] & 0xffff) / 255) & 0xff);
            index++;
        }
    }
    
    public static final RGB24Image toRGB24(RGB48Image in) { 
       
        short [] src = (short[]) in.getData();
        byte [] out = new byte[src.length];
        
        RGB48toRGB24(src, out);

        return new RGB24Image(in.number, in.width, in.height, in.metaData, out);   
    }
    
    public static final RGB48Image toRGB48(RGB24Image in) { 
        
        byte [] src = (byte[]) in.getData();
        short [] out = new short[src.length];
        
        RGB24toRGB48(src, out);

        return new RGB48Image(in.number, in.width, in.height, in.metaData, out);   
    }
    
    
    public static final RGBImage toRGBImage(long number, Object meta, Raster r) { 
        
        final int height = r.getHeight();
        final int width = r.getWidth();
        
        PixelInterleavedSampleModel sm = 
            (PixelInterleavedSampleModel) r.getSampleModel();
        
        DataBuffer buf = r.getDataBuffer();
        
        int banks = buf.getNumBanks();       
        
        if (banks != 1) {
            throw new RuntimeException("Unsupported image buffer type!");
        }

        int type = buf.getDataType();
        
        if (type == DataBuffer.TYPE_USHORT) { 

            int stride = sm.getPixelStride();

            short [] data = ((DataBufferUShort) buf).getData();
            short [] result = new short[width*height*3];
            
            if (stride == 3) {
                System.arraycopy(data, 0, result, 0, width*height*3);
            } else if (stride == 4) {
                RGB64toRGB48(data, result);
            } else { 
                throw new RuntimeException("Unsupported image buffer type!");
            }
           
            return new RGB48Image(number, width, height, meta, result);
       
        } else if (type == DataBuffer.TYPE_BYTE) { 
            
            int stride = sm.getPixelStride();

            byte [] data = ((DataBufferByte) buf).getData();
            byte [] result = new byte[width*height*3];
            
            if (stride == 3) {
                System.arraycopy(data, 0, result, 0, width*height*3);
            } else if (stride == 4) {
                RGB32toRGB24(data, result);
            } else { 
                throw new RuntimeException("Unsupported image buffer type!");
            }
            
            return new RGB24Image(number, width, height, meta, result);
        } else if (type == DataBuffer.TYPE_INT) { 
            
          //  System.out.println("GOT IMAGE OF TYPE INT");
            
            throw new RuntimeException("Unsupported image buffer type (INT)!");
        } else {
          
          //  System.out.println("GOT IMAGE OF TYPE OTHER");
            
            throw new RuntimeException("Unsupported image buffer type (other)!");         
        }
    }


    public static Raster toRaster(UncompressedImage image) {
        
        /*
        
        if (image instanceof RGB48Image) { 
            
            short [] data = (short[]) image.getData();
            
            DataBufferUShort buf = new DataBufferUShort(data, data.length);
            
          //  PixelInterleavedSampleModel model = 
          //      new PixelInterleavedSampleModel(DataBuffer.TYPE_USHORT, 
          //              image.width, image.height, 3, image.width*3, 
           //             new int [] { 0, 1, 2 });
          
            ComponentSampleModel model = 
                new ComponentSampleModel(DataBuffer.TYPE_USHORT, 
                        image.width, image.height, 3, image.width*3, 
                        new int [] { 0, 1, 2 });
            
            return Raster.createRaster(model, buf, new Point(0,0));
        
        } else if (image instanceof RGB24Image) { 
            
            byte [] data = (byte []) image.getData();
          
            DataBufferByte buf = new DataBufferByte(data, data.length);
           
            ComponentSampleModel model = 
                new ComponentSampleModel(DataBuffer.TYPE_BYTE, 
                        image.width, image.height, 3, image.width*3, 
                        new int [] { 0, 1, 2 });
            
            return Raster.createWritableRaster(model, buf, new Point(0,0));
        } 
        
        */
        
        byte [] data = null;
        
        if (image instanceof RGB24Image) { 
            data = (byte []) image.getData();
        
        } else if (image instanceof RGB48Image) { 
            
            // NOTE: we always convert to RGB24 format here, since the 
            // BufferedImage that we usually wrap around a Raster later on 
            // cannot really handle 48 bit data. It can read it from disk, etc., 
            // but I can't seem create one directly!
            
            short [] tmp = (short[]) image.getData();
            data = new byte[tmp.length];
            
            RGB48toRGB24(tmp, data);
        
        } else { 
            throw new RuntimeException("Unsupported image type!");
        }
        
        // System.out.println("ToRaster create data buffer of " + data.length + "bytes!");
        
        DataBufferByte buf = new DataBufferByte(data, data.length);
           
      //  ComponentSampleModel model = 
      //      new ComponentSampleModel(DataBuffer.TYPE_BYTE, 
      //              image.width, image.height, 3, image.width*3, 
       //             new int [] { 0, 1, 2 });
        
        PixelInterleavedSampleModel model = 
              new PixelInterleavedSampleModel(DataBuffer.TYPE_BYTE, 
                      image.width, image.height, 3, image.width*3, 
                     new int [] { 0, 1, 2 });
      
        return Raster.createWritableRaster(model, buf, null);
    }
    
    public static void toRaster(UncompressedImage image, WritableRaster r) {
        
        if (image instanceof RGB24Image) { 
            byte [] data = (byte []) image.getData();
            
            int [] tmp = new int[3];
            
            int index = 0;
            
            for (int h=0;h<image.height;h++) { 
                for (int w=0;w<image.width;w++) { 
                   
                    tmp[0] = data[index++] & 0xff;
                    tmp[1] = data[index++] & 0xff;
                    tmp[2] = data[index++] & 0xff;
                    
                    // TODO horribly slow, but the only thing that seems to 
                    //       work reliably !!!
                    r.setPixel(w, h, tmp);
                }
            }
        } else { 
            throw new RuntimeException("Unsupported image type!");
        }
           
    }

    public static BufferedImage toBufferedImage(UncompressedImage image) {
      
        BufferedImage b = new BufferedImage(image.width, image.height, 
                BufferedImage.TYPE_3BYTE_BGR);
        
        if (image instanceof RGB24Image) { 
            
            byte [] data = (byte[]) image.getData();
            
            int [] tmp = new int[image.width*image.height];
            
            int index = 0;
            int index2 = 0;
            
            for (int h=0;h<image.height;h++) { 
                for (int w=0;w<image.width;w++) { 
                    // TODO horribly slow, but the only thing that seems to 
                    //       work reliably !!!
                    tmp[index2++] = (data[index++] & 0xff) << 16 |
                        (data[index++] & 0xff) << 8 |
                        (data[index++] & 0xff);
                 }
            }
        
            b.setRGB(0, 0, image.width, image.height, tmp, 0, image.width);  
            
            /*
            int index = 0;
            
            for (int h=0;h<image.height;h++) { 
                for (int w=0;w<image.width;w++) { 
                    
                    // TODO horribly slow, but the only thing that seems to 
                    //       work reliably !!!
                    int p = (data[index++] & 0xff) << 16;
                    p |= (data[index++] & 0xff) << 8;
                    p |= (data[index++] & 0xff);
                    
                    b.setRGB(w, h, p);                
                }
            }*/
             
        } else if (image instanceof RGB48Image) { 
            
            short [] data = (short []) image.getData();
            
            int [] tmp = new int[image.width*image.height];
            
            int index = 0;
            int index2 = 0;
            
            for (int h=0;h<image.height;h++) { 
                for (int w=0;w<image.width;w++) { 
                    // TODO horribly slow, but the only thing that seems to 
                    //       work reliably !!!
                   tmp[index2++] = 
                       (((data[index++] & 0xffff) / 255) & 0xFF) << 16 | 
                       (((data[index++] & 0xffff) / 255) & 0xFF) << 8 |
                       (((data[index++] & 0xffff) / 255) & 0xFF);
                 }
            }
        
            b.setRGB(0, 0, image.width, image.height, tmp, 0, image.width);                
            
            /*
            int index = 0;
            
            for (int h=0;h<image.height;h++) { 
                for (int w=0;w<image.width;w++) { 
                    
                    // TODO horribly slow, but the only thing that seems to 
                    //       work reliably !!!
                    int p  = (((data[index++] & 0xffff) / 255) & 0xFF) << 16;
                        p |= (((data[index++] & 0xffff) / 255) & 0xFF) << 8;
                        p |= (((data[index++] & 0xffff) / 255) & 0xFF);
                    
                    b.setRGB(w, h, p);                
                }
            }*/
            
        } else { 
            throw new RuntimeException("Unsupported image type!");
        }
        
        return b;
    }
    
}
