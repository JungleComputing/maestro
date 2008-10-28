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
        TaskType[] allTaskTypes = Globals.allTaskTypes;
	antTypeRoutingTable = new AntTypeRoutingTable[allTaskTypes.length];

        for( int i=0; i<allTaskTypes.length; i++  ) {
            antTypeRoutingTable[i] = new AntTypeRoutingTable( allTaskTypes[i] );
        }
    }

    void update( TaskType type, IbisIdentifier ibis, long timestamp )
    {
        antTypeRoutingTable[type.index].update( ibis, timestamp );
    }

    NodeInfo getWorker( TaskType type )
    {
	return antTypeRoutingTable[type.index].getBestReadyWorker( type );
    }

    void removeNode(IbisIdentifier theIbis)
    {
	for( AntTypeRoutingTable t: antTypeRoutingTable ) {
	    t.removeNode( theIbis );
	}
    }
}
