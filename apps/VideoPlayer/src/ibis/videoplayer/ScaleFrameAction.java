package ibis.videoplayer;

import ibis.maestro.Job;
import ibis.maestro.Node;

/**
 * A job to fetch and scale a frame.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class ScaleFrameAction implements Job
{
    private static final long serialVersionUID = -3938044583266505212L;

    @Override
    public Object run(Object obj, Node node )
    {
	Frame frame = (Frame) obj;
	if( Settings.traceScaler ){
	    System.out.println( "Scaling frame " + frame.frameno );
	}
	short outr[] = new short[frame.r.length/4];
	{
	    int ix = 0;
	    short r[] = frame.r;

	    for( int i=0; i<r.length; i += 4 ){
		short v = (short) ((r[i] + r[i+1] + r[i+2] + r[i+3])/4);
		outr[ix++] = v;
	    }
	}
	short outg[] = new short[frame.g.length/4];
	{
	    int ix = 0;
	    short g[] = frame.g;

	    for( int i=0; i<g.length; i += 4 ){
		short v = (short) ((g[i] + g[i+1] + g[i+2] + g[i+3])/4);
		outg[ix++] = v;
	    }
	}
	short outb[] = new short[frame.b.length/4];
	{
	    int ix = 0;
	    short b[] = frame.b;

	    for( int i=0; i<b.length; i += 4 ){
		short v = (short) ((b[i] + b[i+1] + b[i+2] + b[i+3])/4);
		outb[ix++] = v;
	    }
	}
	if( Settings.traceScaler ){
	    System.out.println( "Scaling frame " + frame.frameno );
	}
	return new Frame( frame.frameno, frame.width/2, frame.height/2, outr, outg, outb );
    }
}
