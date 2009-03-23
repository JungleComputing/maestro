package ibis.videoplayer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

/**
 * A video frame.
 * 
 * @author Kees van Reeuwijk
 */
class RGB48Image extends UncompressedImage {
    private static final long serialVersionUID = 8797700803728846092L;
    static final int BANDS = 3;

    /**
     * The channels of the image. Each channel stores its values consecutively
     * in one large array row by row from top to bottom.
     */
    final short data[];

    RGB48Image(int frameno, int width, int height, short data[]) {
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
        return "frame " + frameno + " RGB48 " + width + "x" + height + "; "
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
     * Compare two images.
     * 
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
        final RGB48Image other = (RGB48Image) obj;
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
        int swidth = width / factor;
        int sheight = height / factor;
        short res[] = new short[swidth * sheight * BANDS];

        int ix = 0;
        for (int y = 0; y < sheight; y++) {
            int oldY = y * factor;
            for (int x = 0; x < swidth; x++) {
                int oldX = x * factor;
                // The sum of the values we're going to average.
                int redValues = 0;
                int greenValues = 0;
                int blueValues = 0;

                // Compute the offset for the first row of pixels.
                int offset = (oldX + oldY * width) * BANDS;
                for (int ypix = 0; ypix < factor; ypix++) {
                    for (int xpix = 0; xpix < factor; xpix += BANDS) {
                        // Convert to unsigned and add to the average.
                        int vr = data[offset + xpix];
                        redValues += (0xFFFF & vr);
                        int vg = data[offset + xpix + 1];
                        greenValues += (0xFFFF & vg);
                        int vb = data[offset + xpix + 2];
                        blueValues += (0xFFFF & vb);
                    }
                    offset += width * BANDS;
                }
                // Store rounded value.
                res[ix++] = (short) (redValues / weight);
                res[ix++] = (short) (greenValues / weight);
                res[ix++] = (short) (blueValues / weight);
            }
        }
        return new RGB48Image(frameno, swidth, sheight, res);
    }

    @Override
    Image colourCorrect(double frr, double frg, double frb, double fgr,
            double fgg, double fgb, double fbr, double fbg, double fbb) {
        short res[] = new short[width * height * BANDS];

        // Apply the color correction matrix
        for (int i = 0; i < data.length; i += BANDS) {
            double vr = frr * data[i] + frg * data[i + 1] + frb * data[i + 2];
            double vg = fgr * data[i] + fgg * data[i + 1] + fgb * data[i + 2];
            double vb = fbr * data[i] + fbg * data[i + 1] + fbb * data[i + 2];

            res[i] = (short) vr;
            res[i + 1] = (short) vg;
            res[i + 2] = (short) vb;
        }
        if (Settings.traceJobs) {
            System.out.println("Color-corrected " + this);
        }
        return new RGB48Image(frameno, width, height, res);
    }

    /**
     * Writes this image to the given file.
     * 
     * @param f
     *            The file to write to.
     * @throws IOException
     *             Thrown if the image cannot be written.
     */
    @Override
    void write(File f) throws IOException {
        FileOutputStream stream = new FileOutputStream(f);
        String header = "P6\n" + width + ' ' + height + "\n65535\n";
        stream.write(header.getBytes());
        byte buffer[] = new byte[2 * BANDS * width];
        int ix = 0;
        for (int h = 0; h < height; h++) {
            int bufix = 0;
            for (int w = 0; w < width * BANDS; w++) {
                int v = data[ix++];
                buffer[bufix++] = (byte) ((v >> 8) & 0xFF);
                buffer[bufix++] = (byte) (v & 0xFF);
            }
            stream.write(buffer);
        }
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
        stream.println("RGB48 " + width + "x" + height + " frame " + frameno);
        int ix = 0;
        for (int h = 0; h < height; h++) {
            for (int w = 0; w < width; w++) {
                stream.format("%04x %04x %04x\n", data[ix++], data[ix++],
                        data[ix++]);
            }
            stream.println();
        }
        stream.close();
    }

    static RGB48Image buildConstantImage(int frameno, int width, int height,
            int vr, int vg, int vb) {
        short res[] = new short[width * height * BANDS];

        int ix = 0;
        for (int h = 0; h < height; h++) {
            for (int w = 0; w < width; w++) {
                res[ix++] = (short) vr;
                res[ix++] = (short) vg;
                res[ix++] = (short) vb;
            }
        }
        return new RGB48Image(frameno, width, height, res);
    }

    static RGB48Image buildGradientImage(int frameno, int width, int height) {
        short res[] = new short[width * height * BANDS];
        int ix = 0;
        short vg = 0;

        for (int h = 0; h < height; h++) {
            short vr = 0;
            for (int w = 0; w < width; w++) {
                res[ix++] = vr;
                res[ix++] = vg;
                res[ix++] = 127 * 256;
                vr += 200;
            }
            vg += 200;
        }
        return new RGB48Image(frameno, width, height, res);
    }

    private static byte[] makeByteSamples(short a[]) {
        byte res[] = new byte[a.length];

        for (int i = 0; i < a.length; i++) {
            int val = (a[i] & 0xFFFF);
            res[i] = (byte) (val / 256);
        }
        return res;
    }

    RGB24Image buildRGB24Image() {
        byte res[] = makeByteSamples(data);

        return new RGB24Image(frameno, width, height, res);
    }

    static RGB48Image convert(Image img) {
        if (img instanceof RGB48Image) {
            // Now this is easy.
            return (RGB48Image) img;
        }
        System.err.println("Don't know how to convert a " + img.getClass()
                + " to a RGB48 image");
        return null;
    }

    /**
     * 
     * @param factor
     * @return
     */

    @Override
    UncompressedImage scaleUp(int factor) {
        // FIXME Auto-generated method stub
        return null;
    }

    /**
     * 
     * @return
     */

    @Override
    UncompressedImage sharpen() {
        // FIXME Auto-generated method stub
        return null;
    }

	@Override
	UncompressedImage getVerticalSlice(int start, int end) {
        int fromByte = start * BANDS * width;
        int toByte = end * BANDS * width;
        short res[] = Arrays.copyOfRange(data, fromByte, toByte);
        return new RGB48Image(frameno, width, end-start, res);
	}

	public static RGB48Image concatenateImagesVertically(
			UncompressedImage[] fragments) {
		// FIXME: implement this
		return null;
	}

}