package demo.grid;

import ibis.ipl.IbisIdentifier;

import java.io.Serializable;

public abstract class Request implements Serializable {

    public abstract Object execute(IbisIdentifier local);
    
    
}
