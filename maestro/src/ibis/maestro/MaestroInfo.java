package ibis.maestro;

import ibis.ipl.IbisIdentifier;

/** The list of maestros in this computation. */
class MaestroInfo {
    IbisIdentifier ibis;   // The identifier of the maestro.

    MaestroInfo( IbisIdentifier id ) {
        this.ibis = id;
    }
}