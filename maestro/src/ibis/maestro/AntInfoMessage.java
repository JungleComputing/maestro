package ibis.maestro;

import java.util.ArrayList;

/**
 * @author Kees van Reeuwijk
 *
 */
public class AntInfoMessage extends Message
{
    private static final long serialVersionUID = 1L;
    
    final ArrayList<AntPoint> antPoints;

    AntInfoMessage( ArrayList<AntPoint> antPoints )
    {
        this.antPoints = antPoints;
    }
}
