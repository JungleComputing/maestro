package ibis.videoplayer;

import ibis.maestro.AtomicJob;

import java.io.Serializable;

/**
 * A job to decompress a frame. We fake decompressing a video frame by simply
 * doubling the frame and repeating the content. FIXME: implement this properly.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public class DecompressFrameJob implements AtomicJob {
    private static final long serialVersionUID = -3938044583266505212L;

    /**
     * How many times should I repeat the fake decompression loop to approximate
     * the real decompression process.
     */
    private static final int REPEAT = 2;

    /**
     * Runs this job.
     * 
     * @return The decompressed frame.
     */
    @Override
    public Serializable run(Object obj) {
        final RGB48Image frame = (RGB48Image) obj;
        final short in[] = frame.data;
        final short data[] = new short[in.length * REPEAT * REPEAT];

        if (Settings.traceDecompressor) {
            System.out.println("Decompressing frame " + frame.frameno);
        }
        int outix = 0;

        // FIXME: do something sane here.
        for (int y = 0; y < frame.height; y++) {
            final int base = y * frame.width;

            for (int ry = 0; ry < REPEAT; ry++) {
                for (int x = 0; x < frame.width; x++) {
                    final int ix = base + x;
                    for (int rx = 0; rx < REPEAT; rx++) {
                        data[outix++] = in[ix];
                    }
                }
            }
        }
        return new RGB48Image(frame.frameno, frame.width * REPEAT, frame.height
                * REPEAT, data);
    }

    /**
     * @return True, because this job can run anywhere.
     */
    @Override
    public boolean isSupported() {
        return true;
    }
}
