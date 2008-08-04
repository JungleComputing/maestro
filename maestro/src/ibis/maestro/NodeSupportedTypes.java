package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.Serializable;

class NodeSupportedTypes implements Serializable {
    private static final long serialVersionUID = 1L;
    final IbisIdentifier ibis;
    final TaskType types[];

    /**
     * @param ibis
     * @param types
     */
    private NodeSupportedTypes(IbisIdentifier ibis, TaskType[] types) {
	this.ibis = ibis;
	this.types = types;
    }
}
