package net;

import ibis.ipl.IbisIdentifier;

public interface ManagementCallback {
    public void register(IbisIdentifier id, Object data);
    public void registered(IbisIdentifier id, Object data);    
    public void managementMessage(IbisIdentifier id, Object data);
}
