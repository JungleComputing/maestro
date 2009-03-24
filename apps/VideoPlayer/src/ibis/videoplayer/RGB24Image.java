package ibis.videoplayer;

import java.awt.Transparency;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.PixelInterleavedSampleModel;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;

/**
 * A video frame.
 * 
 * @author Kees van Reeuwijk
 */
class RGB24Image extends UncompressedImage {
    private static final long serialVersionUID = 8797700803728846092L;

    static final int BANDS = 3;

    /**
     * The data of the image. We currently assume that there are three bands in
     * there, each with one unsigned byte of value, with the meaning Red, Green,
     * Blue.
     */
    final byte data[];

    RGB24Image(int frameno, int width, int height, byte[] data) {
        super(width, height, frameno);
        this.data = data;
    }

    /**
     * Returns a string representation of this frame.
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        return "frame " + frameno + " RGB24 " + width + "x" + height + "; "
                + BANDS + " bands";
    }

    /**
     * @return The hash code of this image.
     */
    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    /**
     * @param obj
     *            The image to compare to.
     * @return True iff the two images are equal.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final RGB24Image other = (RGB24Image) obj;
        return Arrays.equals(data, other.data);
    }

    /**
     * Make sure the given dimension is a multiple of the given factor. If not,
     * use the given name in any error message we generate.
     * 
     * @param dim
     *            The dimension to check.
     * @param name
     *            The name of the dimension.
     * @param factor
     *            The factor that should occur in the dimension
     * @return True iff every checks out ok.
     */
    private static boolean checkFactor(int dim, String name, int factor) {
        if (((dim / factor) * factor) != dim) {
            System.err.println("The " + name + " of this image (" + dim
                    + ") is not a multiple of " + factor);
            return false;
        }
        return true;
    }

    @Override
    UncompressedImage scaleDown(int factor) {
        if (Settings.traceScaler) {
            System.out.println("Scaling down " + this);
        }
        if (!checkFactor(width, "width", factor)) {
            return null;
        }
        if (!checkFactor(height, "height", factor)) {
            return null;
        }
        int weight = factor * factor;
        int wt2 = weight / 2; // Used for rounding.
        int swidth = width / factor;
        int sheight = height / factor;
        byte res[] = new byte[swidth * sheight * BANDS];

        int ix = 0;
        for (int y = 0; y < sheight; y++) {
            int oldY = y * factor;
            for (int x = 0; x < swidth; x++) {
                int oldX = x * factor;
                int offset = (oldX + oldY * width) * BANDS;
                int redValues = 0; // The sum of the red values we're going to
                // average.
                int greenValues = 0;
                int blueValues = 0;

                // Compute the offset in the channel for the first row of
                // pixels.
                for (int ypix = 0; ypix < factor; ypix++) {
                    for (int xpix = 0; xpix < factor; xpix += BANDS) {
                        int vr = data[offset + xpix];
                        int vg = data[offset + xpix + 1];
                        int vb = data[offset + xpix + 2];

                        // Convert to unsigned and add to the average.
                        redValues += (0xFFFF & vr);
                        greenValues += (0xFFFF & vg);
                        blueValues += (0xFFFF & vb);
                    }
                    offset += width * BANDS;
                }
                // Store rounded values.
                res[ix++] = (byte) ((redValues + wt2) / weight);
                res[ix++] = (byte) ((greenValues + wt2) / weight);
                res[ix++] = (byte) ((blueValues + wt2) / weight);
            }
        }
        return new RGB24Image(frameno, swidth, sheight, res);
    }

    /**
     * Round the given value to the nearest byte value, but make sure we don't
     * overflow the byte.
     * 
     * @param v
     *            The value to round.
     * @return The rounded value.
     */
    private static final byte round(double v) {
        int i = (int) Math.round(v);
        return (byte) (Math.min(255, i));
    }

    /**
     * Given a byte returns an unsigned int.
     * 
     * @param v
     * @return
     */
    private static int byteToInt(byte v) {
        return v & 0xFF;
    }

    private static double interpolate(double f, double v0, double v1) {
        return f * v1 + (1 - f) * v0;
    }

    private static byte applyConvolution(byte v00, byte v01, byte v02,
            byte v10, byte v11, byte v12, byte v20, byte v21, byte v22,
            int k00, int k01, int k02, int k10, int k11, int k12, int k20,
            int k21, int k22, int weight) {
        int val = k00 * byteToInt(v00) + k10 * byteToInt(v10) + k20
                * byteToInt(v20) + k01 * byteToInt(v01) + k11 * byteToInt(v11)
                + k21 * byteToInt(v21) + k02 * byteToInt(v02) + k12
                * byteToInt(v12) + k22 * byteToInt(v22);
        return (byte) Math.min(255, Math.max(0, (val + weight / 2) / weight));
    }

    /**
     * Applies the kernel with the given factors to an image, and returns a new
     * image.
     * 
     * @return The image with the kernel applied.
     */
    private RGB24Image convolution3x3(int k00, int k01, int k02, int k10,
            int k11, int k12, int k20, int k21, int k22, int weight) {
        byte res[] = new byte[width * height * BANDS];
        final int rowBytes = width * BANDS;

        // Copy the top and bottom line into the result image.
        System.arraycopy(data, 0, res, 0, rowBytes);
        System.arraycopy(data, (height - 1) * rowBytes, res, (height - 1)
                * rowBytes, rowBytes);

        /** Apply kernal to the remaining rows. */
        int wix = rowBytes; // Skip first row.
        byte r00, r01, r02, r10, r11, r12, r20, r21, r22;
        byte g00, g01, g02, g10, g11, g12, g20, g21, g22;
        byte b00, b01, b02, b10, b11, b12, b20, b21, b22;
        for (int h = 1; h < (height - 1); h++) {
            int rix = rowBytes * h;
            r00 = data[rix - rowBytes];
            r01 = data[rix];
            r02 = data[rix + rowBytes];
            rix++;
            g00 = data[rix - rowBytes];
            g01 = data[rix];
            g02 = data[rix + rowBytes];
            rix++;
            b00 = data[rix - rowBytes];
            b01 = data[rix];
            b02 = data[rix + rowBytes];
            rix++;
            r10 = data[rix - rowBytes];
            r11 = data[rix];
            r12 = data[rix + rowBytes];
            rix++;
            g10 = data[rix - rowBytes];
            g11 = data[rix];
            g12 = data[rix + rowBytes];
            rix++;
            b10 = data[rix - rowBytes];
            b11 = data[rix];
            b12 = data[rix + rowBytes];
            rix++;
            // Write the left border pixel.
            res[wix++] = r00;
            res[wix++] = g00;
            res[wix++] = b00;
            for (int w = 1; w < (width - 1); w++) {
                r20 = data[rix - rowBytes];
                r21 = data[rix];
                r22 = data[rix + rowBytes];
                rix++;
                g20 = data[rix - rowBytes];
                g21 = data[rix];
                g22 = data[rix + rowBytes];
                rix++;
                b20 = data[rix - rowBytes];
                b21 = data[rix];
                b22 = data[rix + rowBytes];
                rix++;
                res[wix++] = applyConvolution(r00, r01, r02, r10, r11, r12,
                        r20, r21, r22, k00, k01, k02, k10, k11, k12, k20, k21,
                        k22, weight);
                res[wix++] = applyConvolution(g00, g01, g02, g10, g11, g12,
                        g20, g21, g22, k00, k01, k02, k10, k11, k12, k20, k21,
                        k22, weight);
                res[wix++] = applyConvolution(b00, b01, b02, b10, b11, b12,
                        b20, b21, b22, k00, k01, k02, k10, k11, k12, k20, k21,
                        k22, weight);
                r00 = r10;
                r10 = r20;
                r01 = r11;
                r11 = r21;
                r02 = r12;
                r12 = r22;
                g00 = g10;
                g10 = g20;
                g01 = g11;
                g11 = g21;
                g02 = g12;
                g12 = g22;
                b00 = b10;
                b10 = b20;
                b01 = b11;
                b11 = b21;
                b02 = b12;
                b12 = b22;
            }
            // Write the right border pixel.
            res[wix++] = r11;
            res[wix++] = g11;
            res[wix++] = b11;
        }
        return new RGB24Image(frameno, width, height, res);
    }

    @Override
    RGB24Image sharpen() {
        return convolution3x3(-1, -1, -1, -1, 9, -1, -1, -1, -1, 1);
    }

    RGB24Image blur() {
        return convolution3x3(1, 1, 1, 1, 1, 1, 1, 1, 1, 9);
    }

    /**
     * Given an interpolation factor and a start and end value, returns the
     * interpolated value.
     * 
     * @param factor
     *            The interpolation factor. 0.0 is start value, 1.0 is end
     *            value, values inbetween give a proportional mix.
     * @param start
     *            The start value.
     * @param end
     *            The end value.
     * @return The interpolated value.
     */
    private static byte interpolate(double fx, double fy, byte v00, byte v10,
            byte v01, byte v11) {
        double res = interpolate(fx, interpolate(fy, byteToInt(v00),
                byteToInt(v01)),
                interpolate(fy, byteToInt(v10), byteToInt(v11)));
        return round(res);
    }

    /**
     * Scales up the image by the given multiplication factor. This factor is
     * applied in both directions, so the new image will have
     * <code>factor*factor</code> times as many pixels.
     * 
     * @return The scaled up image.
     */
    @Override
    RGB24Image scaleUp(int factor) {
        if (Settings.traceScaler) {
            System.out.println("Scaling up " + this);
        }
        int swidth = width * factor;
        int sheight = height * factor;
        byte res[] = new byte[swidth * sheight * BANDS];

        int fillix;
        int rowstart = 0;
        for (int y = 0; y < height; y++) {
            int nextrow = rowstart + BANDS * width;

            for (int x = 0; x < width; x++) {
                int readIx = rowstart + x * BANDS;
                byte red10, green10, blue10;
                byte red01, green01, blue01;
                byte red11, green11, blue11;

                // Top left pixel
                byte red00 = data[readIx++];
                byte green00 = data[readIx++];
                byte blue00 = data[readIx++];

                // Top right pixel
                if ((x + 1) < width) {
                    red10 = data[readIx++];
                    green10 = data[readIx++];
                    blue10 = data[readIx++];
                } else {
                    // That's beyond the width of the image; use the border
                    // pixel.
                    red10 = red00;
                    green10 = green00;
                    blue10 = blue00;
                }

                if ((y + 1) < height) {
                    readIx = nextrow + x * BANDS;

                    // Bottom left pixel
                    red01 = data[readIx++];
                    green01 = data[readIx++];
                    blue01 = data[readIx++];

                    if ((x + 1) < width) {
                        // Bottom right pixel
                        red11 = data[readIx++];
                        green11 = data[readIx++];
                        blue11 = data[readIx++];
                    } else {
                        // That's beyond the width of the image; use the border
                        // pixel.
                        red11 = red01;
                        green11 = green01;
                        blue11 = blue01;
                    }
                } else {
                    // That's beyond the height of the image; use the border
                    // pixels.
                    red01 = red00;
                    green01 = green00;
                    blue01 = blue00;
                    red11 = red10;
                    green11 = green10;
                    blue11 = blue10;
                }
                for (int iy = 0; iy < factor; iy++) {
                    double intery = ((double) iy) / ((double) factor);

                    fillix = BANDS * (x * factor + (y * factor + iy) * swidth);
                    for (int ix = 0; ix < factor; ix++) {
                        double interx = ((double) ix) / ((double) factor);

                        res[fillix++] = interpolate(interx, intery, red00,
                                red10, red01, red11);
                        res[fillix++] = interpolate(interx, intery, green00,
                                green10, green01, green11);
                        res[fillix++] = interpolate(interx, intery, blue00,
                                blue10, blue01, blue11);
                    }
                }
            }
            rowstart = nextrow;
        }
        return new RGB24Image(frameno, swidth, sheight, res);
    }

    @Override
    Image colourCorrect(double frr, double frg, double frb, double fgr,
            double fgg, double fgb, double fbr, double fbg, double fbb) {
        byte res[] = new byte[width * height * BANDS];

        // Apply the color correction matrix
        for (int i = 0; i < data.length; i += BANDS) {
            double vr = frr * data[i] + frg * data[i + 1] + frb * data[i + 2];
            double vg = fgr * data[i] + fgg * data[i + 1] + fgb * data[i + 2];
            double vb = fbr * data[i] + fbg * data[i + 1] + fbb * data[i + 2];

            res[i] = round(vr);
            res[i + 1] = round(vg);
            res[i + 2] = round(vb);
        }
        if (Settings.traceJobs) {
            System.out.println("Color-corrected " + this);
        }
        return new RGB24Image(frameno, width, height, res);
    }

    /**
     * Returns a JPeg compressed version of this image.
     * 
     * @throws IOException
     *             Thrown if the conversion causes an I/O error
     */
    JpegCompressedImage toJpegImage() throws IOException {
        DataBuffer buffer = new DataBufferByte(data, data.length);
        int offsets[] = new int[] { 0, 1, 2 };
        SampleModel sampleModel = new PixelInterleavedSampleModel(buffer
                .getDataType(), width, height, BANDS, BANDS * width, offsets);
        int bits[] = new int[] { 8, 8, 8 };
        ColorModel colorModel = new ComponentColorModel(ColorSpace
                .getInstance(ColorSpace.CS_sRGB), bits, false, false,
                Transparency.OPAQUE, buffer.getDataType());
        BufferedImage img = new BufferedImage(colorModel, Raster
                .createWritableRaster(sampleModel, buffer, null), false, null);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        ImageIO.setUseCache(false);
        ImageOutputStream output = ImageIO.createImageOutputStream(out);
        Iterator<?> writers = ImageIO.getImageWritersByFormatName("jpg");

        if (writers == null || !writers.hasNext()) {
            throw new RuntimeException("No jpeg writers!");
        }
        ImageWriter writer = (ImageWriter) writers.next();
        writer.setOutput(output);
        writer.write(img);
        writer.dispose();
        output.close();
        out.close();

        byte[] tmp = out.toByteArray();

        // System.out.println("Compression " + image.width + "x" + image.height
        // + " from " + image.getSize() + " to " + tmp.length + " bytes.");

        return new JpegCompressedImage(width, height, frameno, tmp);
    }

    /**
     * Writes this image to the given file. The file will be written in ppm
     * format.
     * 
     * @param f
     *            The file to write to.
     */
    @Override
    void write(File f) throws IOException {
        FileOutputStream stream = new FileOutputStream(f);
        String header = "P6\n" + width + ' ' + height + "\n255\n";
        stream.write(header.getBytes());
        // Since the format of PPM pixels is the same, we can just write
        // all the bytes directly.
        stream.write(data);
        stream.close();
    }

    /**
     * Prints a text dump of this image to the given file.
     * 
     * @param f
     *            The file to write to.
     * @throws IOException
     *             Thrown if the image cannot be written.
     */
    @Override
    void print(File f) throws IOException {
        PrintStream stream = new PrintStream(new FileOutputStream(f));
        stream.println("RGB24 " + width + "x" + height + " frame " + frameno);
        int ix = 0;
        for (int h = 0; h < height; h++) {
            for (int w = 0; w < width; w++) {
                stream.format("%02x %02x %02x\n", data[ix++], data[ix++],
                        data[ix++]);
            }
            stream.println();
        }
        stream.close();
    }

    static RGB24Image buildConstantImage(int frameno, int width, int height,
            int vr, int vg, int vb) {
        byte data[] = new byte[width * height * BANDS];

        int ix = 0;
        for (int h = 0; h < height; h++) {
            for (int w = 0; w < width; w++) {
                data[ix++] = (byte) vr;
                data[ix++] = (byte) vg;
                data[ix++] = (byte) vb;
            }
        }
        return new RGB24Image(frameno, width, height, data);
    }

    static RGB24Image buildGradientImage(int frameno, int width, int height,
            byte startr, byte startg, byte startb) {
        byte data[] = new byte[width * height * BANDS];
        int ix = 0;
        byte vg = startg;

        for (int h = 0; h < height; h++) {
            byte vr = startr;
            for (int w = 0; w < width; w++) {
                data[ix++] = vr;
                data[ix++] = vg;
                data[ix++] = startb;
                vr++;
            }
            vg++;
        }
        return new RGB24Image(frameno, width, height, data);
    }

    static RGB24Image convert(Image img) {
        if (img instanceof RGB24Image) {
            // Now this is easy.
            return (RGB24Image) img;
        }
        if (img instanceof RGB48Image) {
            RGB48Image img48 = (RGB48Image) img;

            return img48.buildRGB24Image();
        }
        System.err.println("Don't know how to convert a " + img.getClass()
                + " to a RGB24 image");
        return null;
    }

    @Override
    UncompressedImage getVerticalSlice(int start, int end) {
        int fromByte = start * BANDS * width;
        int toByte = end * BANDS * width;
        byte res[] = Arrays.copyOfRange(data, fromByte, toByte);
        return new RGB24Image(frameno, width, end - start, res);
    }

    public static RGB24Image concatenateImagesVertically(
            UncompressedImage[] fragments) {
        int width = fragments[0].width;
        int height = 0;
        for (UncompressedImage img : fragments) {
            height += img.height;
            if (img.width != width) {
                System.err
                        .println("Not all concatenated images have the same width");
                return null;
            }
        }
        byte res[] = new byte[width * height * BANDS];
        int fillix = 0;
        for (UncompressedImage img : fragments) {
            RGB24Image img24 = (RGB24Image) img;
            int sz = img.height * width * BANDS;
            System.arraycopy(img24.data, 0, res, fillix, sz);
            fillix += sz;
        }
        return new RGB24Image(fragments[0].frameno, width, height, res);
    }
}