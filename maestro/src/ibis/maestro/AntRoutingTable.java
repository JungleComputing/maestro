package ibis.maestro;

/**
 * The table of routing information for ant routing.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class AntRoutingTable
{
    AntTypeRoutingTable antTypeRoutingTable[];
    
    AntRoutingTable( )
    {
        antTypeRoutingTable = new AntTypeRoutingTable[Globals.allTaskTypes.length];
    }
}
