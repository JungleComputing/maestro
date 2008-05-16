package ibis.videoplayer;

import ibis.maestro.Job;
import ibis.maestro.Node;

/**
 * An action to color-correct a frame. We fake this by a video frame
 * by simply doubling the frame and repeating the content.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class ColorCorrectAction implements Job
{
    private static final long serialVersionUID = -3938044583266505212L;

    /** Runs this job.
     * @return The decompressed frame.
     */
    @Override
    public Object run(Object obj, Node node )
    {
	Frame frame = (Frame) obj;

	double frr = 0.0, frg = 0.0, frb = 1.0;
	double fgr = 0.0, fgg = 1.0, fgb = 0.0;
	double fbr = 1.0, fbg = 0.0, fbb = 0.0;
	short r[] = frame.r;
	short g[] = frame.g;
	short b[] = frame.b;

	// Apply the color correction matrix 
	// We blindly assume r,g, and b, have the same length.
	for( int i=0; i<r.length; i++ ) {
	    double vr = frr*r[i] + frg*g[i] + frb*b[i];
	    double vg = fgr*r[i] + fgg*g[i] + fgb*b[i];
	    double vb = fbr*r[i] + fbg*g[i] + fbb*b[i];
	    
	    r[i] = (short) vr;
	    g[i] = (short) vg;
	    g[i] = (short) vb;
	}
	if( Settings.traceActions ) {
	    System.out.println( "Color-corrected " + frame );
	}
	return new Frame( frame.frameno, frame.width, frame.height, r, g, b );
    }

}
