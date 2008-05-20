package ibis.videoplayer;

import java.awt.image.BandedSampleModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferUShort;
import java.awt.image.Raster;
import java.awt.image.SampleModel;

import javax.imageio.IIOImage;
import javax.imageio.metadata.IIOMetadata;

/**
 * A video frame.
 * 
 * @author Kees van Reeuwijk
 */
class RGB48Image extends UncompressedImage {
    private static final long serialVersionUID = 8797700803728846092L;
    final short r[];
    final short g[];
    final short b[];

    RGB48Image( int frameno, int width, int height, short r[], short g[], short b[] )
    {
        super( width, height, frameno );
	this.r = r;
	this.g = g;
	this.b = b;
    }
    
    /**
     * Returns a string representation of this frame.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
	return "frame " + frameno + " " + width + "x" + height;
    }

    @Override
    Image scale()
    {
        if( Settings.traceScaler ){
            System.out.println( "Scaling frame " + frameno );
        }
        short outr[] = new short[r.length/4];
        {
            int ix = 0;

            for( int i=0; i<r.length; i += 4 ){
                short v = (short) ((r[i] + r[i+1] + r[i+2] + r[i+3])/4);
                outr[ix++] = v;
            }
        }
        short outg[] = new short[g.length/4];
        {
            int ix = 0;

            for( int i=0; i<g.length; i += 4 ){
                short v = (short) ((g[i] + g[i+1] + g[i+2] + g[i+3])/4);
                outg[ix++] = v;
            }
        }
        short outb[] = new short[b.length/4];
        {
            int ix = 0;

            for( int i=0; i<b.length; i += 4 ){
                short v = (short) ((b[i] + b[i+1] + b[i+2] + b[i+3])/4);
                outb[ix++] = v;
            }
        }
        if( Settings.traceScaler ){
            System.out.println( "Scaling " + this );
        }
        return new RGB48Image( frameno, width/2, height/2, outr, outg, outb );
    }


    Image colorCorrect( double frr, double frg, double frb, double fgr, double fgg, double fgb, double fbr, double fbg, double fbb )
    {
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
            System.out.println( "Color-corrected " + this );
        }
        return new RGB48Image( frameno, width, height, r, g, b );
    }

    @Override
    IIOImage toIIOImage()
    {
        short buffers[][] = new short[][] { r, g, b };
        DataBuffer buffer = new DataBufferUShort( buffers, r.length );
        SampleModel sampleModel = new BandedSampleModel( buffer.getDataType(), width, height, 3 );
        Raster raster = Raster.createRaster( sampleModel, buffer, null );
        IIOMetadata metadata = null;
        IIOImage image = new IIOImage( raster, null, metadata ); 
        //return new BufferedImage( raster );
        // FIXME: somehow create a buffered image.
        return image;
    }
}