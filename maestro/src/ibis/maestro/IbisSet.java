package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.util.HashSet;
import java.util.Set;

class IbisSet {
	private final Set<IbisIdentifier> set = new HashSet<IbisIdentifier>();

	synchronized void add( IbisIdentifier ibis )
	{
		set.add( ibis );
	}

	synchronized boolean contains( IbisIdentifier ibis )
	{
		return set.contains( ibis );
	}
}
