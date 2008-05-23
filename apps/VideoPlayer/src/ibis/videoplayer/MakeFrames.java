package ibis.videoplayer;

/**
 * Build the frames for a video sequence. Requires the 'convert' command from imagemagick.
 *
 * @author Kees van Reeuwijk.
 */
class MakeFrames
{
    static final int WIDTH = 3480;
    static final int HEIGHT = 2160;
    static final int ANGLESTEP = 10;
    static final int FRAMES = 30;
    static final int FULL_CIRCLE_FRAMES = 25;
    static final String BACKGROUND_COLOR = "skyblue";

    /** Command line interface.
     * @param args The command line arguments
     */
    public static void main( String[] args )
    {
        for( int frame=0; frame<FRAMES; frame++ ) {
            double phi = (2*Math.PI*frame)/FULL_CIRCLE_FRAMES;
            int centerx = WIDTH/2;
            int centery = HEIGHT/2;
            int l = (4*HEIGHT)/10;
            int endx = centerx + (int) (l*Math.sin( phi ));
            int endy = centery + (int) (l*Math.cos( phi ));
            String filename = String.format( "frame-%05d.png", frame );
            String command = "convert -depth 16 +compress -size " + WIDTH + 'x' + HEIGHT + " xc:" + BACKGROUND_COLOR + " -fill white -stroke black -draw \"stroke-width 10 stroke-linecap round line " + centerx + ',' + centery + ' ' +  endx + ',' + endy + "\" " + filename;
            System.out.println( command );
        }
    }

}
