package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/**
 * The table of routing information for ant routing.
 * 
 * @author Kees van Reeuwijk
 *
 */
public class AntRoutingTable
{
    AntTypeRoutingTable antTypeRoutingTable[];
    
    AntRoutingTable()
    {
        antTypeRoutingTable = new AntTypeRoutingTable[Globals.allTaskTypes.length];
    }

    void update( TaskType type, IbisIdentifier ibis, long timestamp )
    {
        antTypeRoutingTable[type.index].update( ibis, timestamp );
    }
}
