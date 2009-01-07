package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

class IbisSet {
	private final Set<IbisIdentifier> set = new CopyOnWriteArraySet<IbisIdentifier>();

	void add( IbisIdentifier ibis )
	{
		set.add( ibis );
	}

	boolean contains( IbisIdentifier ibis )
	{
		return set.contains( ibis );
	}
}
