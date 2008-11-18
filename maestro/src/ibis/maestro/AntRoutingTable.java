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

    private void update( int i, IbisIdentifier ibis, long timestamp )
    {
        antTypeRoutingTable[i].update( ibis, timestamp );
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

    void addNode( NodeInfo node )
    {
	for( AntTypeRoutingTable t: antTypeRoutingTable ) {
	    t.addNode( node );
	}
    }

    protected void handleAntPoint( AntPoint p )
    {
	update( p.typeIndex, p.workerIbis, p.timestamp );
    }
}
