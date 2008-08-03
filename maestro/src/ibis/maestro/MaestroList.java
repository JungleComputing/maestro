package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.util.ArrayList;

/**
 * The list of Maestros.
 *
 * @author Kees van Reeuwijk.
 */
class MaestroList
{
    private static final long serialVersionUID = 1L;
    private final ArrayList<MaestroInfo> list = new ArrayList<MaestroInfo>();
    boolean everHadMaestros = false;

    /** Remove the maestro with the given identifier.
     * @param id The identifier of the maestro to remove.
     * @return True iff there are no more maestros left.
     */
    synchronized boolean remove( IbisIdentifier id )
    {
        boolean noMaestrosLeft = false;
        int ix = list.size();

        while( ix>0 ) {
            ix--;
            MaestroInfo m = list.get( ix );
            if( m.ibis.equals( id ) ) {
                list.remove( ix );
                noMaestrosLeft = list.isEmpty();
            }
        }
        return everHadMaestros && noMaestrosLeft;
    }
    
    /** Add a new maestro. */
    synchronized void addMaestro( MaestroInfo info )
    {
        list.add( info );
        everHadMaestros = true;
    }

    synchronized boolean contains( IbisIdentifier ibisIdentifier )
    {
        for( MaestroInfo info: list ) {
            if( info.ibis.equals( ibisIdentifier ) ) {
                return true;
            }
        }
        return false;
    }
}
