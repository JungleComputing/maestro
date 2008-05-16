/*
 * $RCSfile: MYTIFFLZWDecompressor.java,v $
 *
 * 
 * Copyright (c) 2005 Sun Microsystems, Inc. All  Rights Reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met: 
 * 
 * - Redistribution of source code must retain the above copyright 
 *   notice, this  list of conditions and the following disclaimer.
 * 
 * - Redistribution in binary form must reproduce the above copyright
 *   notice, this list of conditions and the following disclaimer in 
 *   the documentation and/or other materials provided with the
 *   distribution.
 * 
 * Neither the name of Sun Microsystems, Inc. or the names of 
 * contributors may be used to endorse or promote products derived 
 * from this software without specific prior written permission.
 * 
 * This software is provided "AS IS," without a warranty of any 
 * kind. ALL EXPRESS OR IMPLIED CONDITIONS, REPRESENTATIONS AND 
 * WARRANTIES, INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE OR NON-INFRINGEMENT, ARE HEREBY
 * EXCLUDED. SUN MIDROSYSTEMS, INC. ("SUN") AND ITS LICENSORS SHALL 
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF 
 * USING, MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS
 * DERIVATIVES. IN NO EVENT WILL SUN OR ITS LICENSORS BE LIABLE FOR 
 * ANY LOST REVENUE, PROFIT OR DATA, OR FOR DIRECT, INDIRECT, SPECIAL,
 * CONSEQUENTIAL, INCIDENTAL OR PUNITIVE DAMAGES, HOWEVER CAUSED AND
 * REGARDLESS OF THE THEORY OF LIABILITY, ARISING OUT OF THE USE OF OR
 * INABILITY TO USE THIS SOFTWARE, EVEN IF SUN HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGES. 
 * 
 * You acknowledge that this software is not designed or intended for 
 * use in the design, construction, operation or maintenance of any 
 * nuclear facility. 
 *
 * $Revision: 1.1 $
 * $Date: 2005/02/11 05:01:48 $
 * $State: Exp $
 */
package image.decompression;

import java.io.IOException;

import javax.imageio.IIOException;

import com.sun.media.imageio.plugins.tiff.BaselineTIFFTagSet;
import com.sun.media.imageio.plugins.tiff.TIFFDecompressor;

public class MYTIFFLZWDecompressor extends TIFFDecompressor {

    private static final boolean DEBUG = false;

    private static final int andTable[] = {
        511, 
        1023,
        2047,
        4095
    };

    int predictor;

    byte[] srcData;
  //  byte[] dstData;

    int srcIndex;
   // int dstIndex;

    int nextData = 0;
    int nextBits = 0;
    
    public long decodeRAW = 0;
    public long decode = 0;
    public long LZW = 0;
    public long tableReinit = 0;

    int tableIndex, bitsToGet = 9;
    
    long bytesIn = 0;
    public long bytesOut = 0;
    
    static final class StringTable { 
        
        private byte [] bytes;
        private int len;

        StringTable(int size) {
            bytes = new byte[size];
            len = 0;
        }
            
        StringTable(byte [] s) { 
            bytes = s;
            this.len = s.length;
        }
        
        void reset() { 
            len = 0;
        }
        
        void set(StringTable t1, StringTable t2) { 
            
           // StringTable t1 = stringTable[index1];
           // StringTable t2 = stringTable[index2];
            
            len = t1.len + 1;
            
            if (len > bytes.length) { 
                byte [] tmp = new byte[len];
                
                System.arraycopy(t1.bytes, 0, tmp, 0, t1.len);
                
                bytes = tmp; //Arrays.copyOf(t1.bytes, len);
            } else { 
                // NOTE: Arraycopy doesn't help here, since we only copy 
                // a few of bytes
                //System.arraycopy(t1.bytes, 0, bytes, 0, len-1);
                
                for (int i=0;i<len-1;i++) { 
                    bytes[i] = t1.bytes[i];
                }
            }

            bytes[len-1] = t2.bytes[0];
        }    
        
        int writeString(byte [] dstData, int dstIndex) {
            if (dstIndex < dstData.length) {
                int maxIndex = Math.min(len, dstData.length - dstIndex);

                // NOTE: Arraycopy doesn't help here, since we only copy 
                // a handfull of bytes
                // System.arraycopy(bytes, 0, dstData, dstIndex, maxIndex);

                for (int i=0;i<maxIndex;i++) { 
                    dstData[dstIndex+i] = bytes[i];
                }
                
                return maxIndex;
            }
            return 0;
        }
    }
    
    private StringTable [] stringTable = new StringTable[2*4096];
    
    public MYTIFFLZWDecompressor(int predictor) throws IIOException {
        super();

        if (predictor != BaselineTIFFTagSet.PREDICTOR_NONE && 
                predictor != 
                    BaselineTIFFTagSet.PREDICTOR_HORIZONTAL_DIFFERENCING) {
            throw new IIOException("Illegal value for Predictor in " +
            "TIFF file");
        }

        if(DEBUG) {
            System.out.println("Using horizontal differencing predictor");
        }

        this.predictor = predictor;
        
        initializeStringTable();
    }
    
    private byte [] sdata = new byte[256*1024];
    private int sdataSize = 256*1024;
    
    public void reset() { 
        resetStringTable();
    }
    
    public void decodeRaw(byte[] b,
            int dstOffset,
            int bitsPerPixel,
            int scanlineStride) throws IOException {

      //  long t1 = System.currentTimeMillis();
        
     //   System.out.println("Samples per pixel " + samplesPerPixel);
     //   System.out.println("Bits per samples " + Arrays.toString(bitsPerSample));
        
        // Check bitsPerSample.
        if (predictor == 
            BaselineTIFFTagSet.PREDICTOR_HORIZONTAL_DIFFERENCING) {
            int len = bitsPerSample.length;
            for(int i = 0; i < len; i++) {
                if(bitsPerSample[i] != 8 && bitsPerSample[i] != 16) {
                    throw new IIOException
                    (bitsPerSample[i] + "-bit samples "+
                            "are not supported for Horizontal "+
                    "differencing Predictor");
                }
            }
        }

        stream.seek(offset);

        if (sdataSize < byteCount) { 
            //System.out.println("realloc sdata! " + sdataSize + " " + byteCount);
            sdata = new byte[2*byteCount];
            sdataSize = 2*byteCount;
        }
        
        int bytes = 0;        
        int read = 0;
        
        while (bytes < byteCount) { 
            read = stream.read(sdata, bytes, byteCount-bytes);
            
            if (read > 0) { 
                bytes += read;
            } else { 
                System.out.println("EEP: read returns " + read);
            }
        } 
        
//        stream.readFully(sdata);

        int bytesPerRow = (srcWidth*bitsPerPixel + 7)/8;

        byte[] buf;
        int bufOffset;
        
        if(bytesPerRow == scanlineStride) {
            buf = b;
            bufOffset = dstOffset;
        } else {
            buf = new byte[bytesPerRow*srcHeight];
            bufOffset = 0;
        }

      //  long t2 = System.currentTimeMillis();
        
        bytesOut += decode(sdata, 0, buf, bufOffset);
        
      //  long t3 = System.currentTimeMillis();
        
        if(bytesPerRow != scanlineStride) {
            if(DEBUG) {
                System.out.println("bytesPerRow != scanlineStride");
            }
            int off = 0;
            for (int y = 0; y < srcHeight; y++) {
                System.arraycopy(buf, off, b, dstOffset, bytesPerRow);
                off += bytesPerRow;
                dstOffset += scanlineStride;
            }
        }
        
     //   long t4 = System.currentTimeMillis();
        
       // decodeRAW += (t2 - t1) + (t4 - t3);
      //  decode += (t3 - t2);
    }

    private final int getAsShort(byte [] data, int index) { 
        return ((data[2*index+1] & 0xFF) << 8) | (data[2*index] & 0xFF);
    }
    
    private final void setAsShort(byte [] data, int index, int value) { 
        data[2*index] = (byte) (value & 0xFF);
        data[2*index + 1] = (byte) ((value & 0xFF00) >> 8);
    }
    
    private final int deLZW(
            byte[] sdata, int srcOffset,
            byte[] ddata, int dstOffset) throws IOException {
        
        this.srcData = sdata;
        this.srcIndex = srcOffset;
        
        
    
        int offset = dstOffset;
        
        this.nextData = 0;
        this.nextBits = 0;

        resetStringTable();

        int oldCode = 0;
        int code = getNextCode();
        
    //    StringTable old;
        
    //    long t1 = System.currentTimeMillis();
        
        while (code != 257) {
            
            if (code == 256) {
                resetStringTable();
                
                code = getNextCode();
            
                if (code == 257) {
                    break;
                }

                offset += stringTable[code].writeString(ddata, offset);
            
            } else { 
                int index = nextTableIndex();
                
                StringTable toAdd; 
                StringTable toWrite;

                if (code < tableIndex) {
                    toWrite = toAdd = stringTable[code];
                } else {
                    toAdd = stringTable[oldCode];
                    toWrite = stringTable[index];
                }
                
                stringTable[index].set(stringTable[oldCode], toAdd);
                offset += toWrite.writeString(ddata, offset);
            }
            
            oldCode = code;
            
            code = getNextCode();
        }
        
        return offset - dstOffset;
    }
    
    public int decode(byte[] sdata, int srcOffset,
            byte[] ddata, int dstOffset) throws IOException {
        
        if (sdata[0] == (byte)0x00 && sdata[1] == (byte)0x01) {
            throw new IIOException
            ("TIFF 5.0-style LZW compression is not supported!");
        }

        //System.out.println("decode " + sdata.length + " " + srcOffset+ " " + ddata.length + " " + dstOffset);
        
        final int res = deLZW(sdata, srcOffset, ddata, dstOffset);

     //   long t2 = System.currentTimeMillis();
        
     //   LZW += (t2 - t1);
       
        if (predictor ==
            BaselineTIFFTagSet.PREDICTOR_HORIZONTAL_DIFFERENCING) {

         //   int bps = bitsPerSample[0];
            
            if (bitsPerSample[0] == 8) { 
                
                for (int j = 0; j < srcHeight; j++) {

                    int count = dstOffset + samplesPerPixel * (j * srcWidth + 1);

                    for (int i = samplesPerPixel; i < srcWidth * samplesPerPixel; i++) {

                        ddata[count] += ddata[count - samplesPerPixel];
                        count++;
                    }
                }

            } else if (bitsPerSample[0] == 16) { 

                for (int j = 0; j < srcHeight; j++) {

                    int count = dstOffset + samplesPerPixel * (j * srcWidth + 1);

              //      System.out.println("16 count " + count + " " + srcWidth + " * " + samplesPerPixel);

                    for (int i = samplesPerPixel; i < srcWidth * samplesPerPixel; i++) {

                        // dstData[count] += dstData[count - samplesPerPixel];
                        int tmp1 = getAsShort(ddata, count - samplesPerPixel);
                        int tmp2 = getAsShort(ddata, count);
                        
                      //  System.out.println(tmp1 + " " + tmp2 + " " + (tmp1 + tmp2));
                        
                        setAsShort(ddata, count, tmp1 + tmp2);
                        count++;
                    }
                }
                
            }
        }

        return res;
    }

    /**
     * Initialize the string table.
     */
    private final void initializeStringTable() {
        for (int i = 0; i < 256; i++) {
            stringTable[i] = new StringTable(new byte [] { (byte) i });
        }

        for (int i = 258; i < stringTable.length; i++) {
            stringTable[i] = new StringTable(32);
        }
        
        tableIndex = 258;
        bitsToGet = 9;
    }
    
    private final void resetStringTable() {
        
        tableReinit++;

        for (int i=258;i<stringTable.length;i++) { 
            stringTable[i].reset();
        }
        
        tableIndex = 258;
        bitsToGet = 9;
    }
    
    private final int nextTableIndex() {
        // Add this new String to the table
        int result = tableIndex++;

        if (tableIndex == 511) {
            bitsToGet = 10;
        } else if (tableIndex == 1023) {
            bitsToGet = 11;
        } else if (tableIndex == 2047) {
            bitsToGet = 12;
        } else if (tableIndex == 4095) {
            bitsToGet = 13;
        } 
        
        return result;
    }
    
    
    // Returns the next 9, 10, 11 or 12 bits
    private final int getNextCode() {
        // Attempt to get the next code. The exception is caught to make
        // this robust to cases wherein the EndOfInformation code has been
        // omitted from a strip. Examples of such cases have been observed
        // in practice.

        try {
            nextData = (nextData << 8) | (srcData[srcIndex++] & 0xff);
            nextBits += 8;

            if (nextBits < bitsToGet) {
                nextData = (nextData << 8) | (srcData[srcIndex++] & 0xff);
                nextBits += 8;
            }

            int code =
                (nextData >> (nextBits - bitsToGet)) & andTable[bitsToGet - 9];
            nextBits -= bitsToGet;

            return code;
        } catch (ArrayIndexOutOfBoundsException e) {
            // Strip not terminated as expected: return EndOfInformation code.
            return 257;
        }
    }
}

