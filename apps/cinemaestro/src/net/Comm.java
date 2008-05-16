package net;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.Hashtable;

public class Comm implements MessageUpcall {

    private static final byte OPCODE_REGISTER   = (byte) 1;
    private static final byte OPCODE_REGISTERED = (byte) 2;
    private static final byte OPCODE_MESSAGE    = (byte) 3;
    
    private static final int TIMEOUT = 60*1000;
    
    private static final PortType portTypeManagement1to1 = new PortType(
            PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT, 
            PortType.RECEIVE_AUTO_UPCALLS,
            PortType.CONNECTION_ONE_TO_ONE);    
    
    private static final PortType portTypeManagementNto1 = new PortType(
            PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT, 
            PortType.RECEIVE_AUTO_UPCALLS,
            PortType.CONNECTION_MANY_TO_ONE);    
    
    private static final PortType portTypeOneToOneIbis = new PortType(
            PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT_IBIS, 
            PortType.RECEIVE_EXPLICIT,
            PortType.CONNECTION_ONE_TO_ONE);    
    
    private static final PortType portTypeOneToOneSUN = new PortType(
            PortType.COMMUNICATION_RELIABLE,
            PortType.SERIALIZATION_OBJECT_SUN, 
            PortType.RECEIVE_EXPLICIT,
            PortType.CONNECTION_ONE_TO_ONE);    
        
    private static final IbisCapabilities ibisCapabilities =
        new IbisCapabilities(IbisCapabilities.ELECTIONS_STRICT);

    private final Ibis ibis;

    private final IbisIdentifier masterID;
    
    private final boolean master;
    
    private final Hashtable<String, ReceivePort> receivePorts = new Hashtable<String, ReceivePort>();    
    private final Hashtable<IbisIdentifier, SendPort> sendPorts = new Hashtable<IbisIdentifier, SendPort>();

    private final ManagementCallback managementCallback;
    
    private boolean register = false;
    
    private final PortType portTypeOneToOne;
    
    public Comm(boolean master, ManagementCallback mc, boolean serIbis) throws Exception {

        this.master = master;
        this.managementCallback = mc;
        
        if (serIbis) { 
            portTypeOneToOne = portTypeOneToOneIbis;
        } else { 
            portTypeOneToOne = portTypeOneToOneSUN;
        }
        
        ibis = IbisFactory.createIbis(ibisCapabilities, null, portTypeOneToOne, 
                portTypeManagement1to1, portTypeManagementNto1);
       
        if (master) { 

            IbisIdentifier tmp = ibis.registry().elect("PipeManiaMaster", TIMEOUT);

            if (!tmp.equals(ibis.identifier())) { 
                throw new Exception("I could not elect myself as master!");
            }

            masterID = tmp;
            
            // As a master, we have a single many-to-1 receiveport for incoming 
            // management connections, and many 1-to-1 sendports for the 
            // outgoing ones. The latter are created dynamically
            
            ReceivePort r = ibis.createReceivePort(portTypeManagementNto1, 
                    "managementIn", this);

            receivePorts.put("managementIn", r);
                
            r.enableConnections();
            r.enableMessageUpcalls();
            
        } else { 
            
            masterID = ibis.registry().getElectionResult("PipeManiaMaster", TIMEOUT);
           
            // As a master, we have a 1-to-1 receiveport for incoming 
            // management connections, a 1-to-1 sendport for the 
            // connection to the master. 
            
            ReceivePort r = ibis.createReceivePort(portTypeManagement1to1, 
                    "managementIn", this);

            receivePorts.put("managementIn", r);
            
            r.enableConnections();
            r.enableMessageUpcalls();
         }
    }
    
    private synchronized boolean getRegister() {
        return register;
    }
    
    public void registerAtMaster(Object data) throws Exception { 
        
        if (master) { 
            throw new Exception("Master cannot register!");
        }
        
        if (register) { 
            throw new Exception("Already send register message!");
        }
        
        SendPort s = ibis.createSendPort(portTypeManagementNto1);
        sendPorts.put(masterID, s);
        
        s.connect(masterID, "managementIn", TIMEOUT, true);
        
        sendManagementMessage(masterID, OPCODE_REGISTER, data);
    
        synchronized (this) {
            register = true;
        }
    }
    
    public void connectToWorker(IbisIdentifier id) throws Exception {
        if (!master) {
            throw new Exception("Only master can connect to workers!");
        }
        
        // As a master, we only add management connections!
        SendPort s = ibis.createSendPort(portTypeManagement1to1);
        sendPorts.put(id, s);
        
        s.connect(id, "managementIn");
    }
    
    
    public void registerWorker(IbisIdentifier id, Object data) throws Exception {
        
        if (!master) {
            throw new Exception("Only master can create new management outputs!");
        }
       
        sendManagementMessage(id, OPCODE_REGISTERED, data);
    }  

    private void sendManagementMessage(IbisIdentifier destination, byte opcode, Object data) throws Exception {
        
        SendPort s = sendPorts.get(destination);
        
        if (s == null) { 
            throw new Exception("Destination " + destination + " not found!");
        }
        
        WriteMessage wm = s.newMessage();
        wm.writeByte(opcode);
        wm.writeObject(data);
        wm.finish();
    }  

    public void sendManagementMessage(IbisIdentifier destination, Object data) throws Exception {        

        if (master) {
            sendManagementMessage(destination, OPCODE_MESSAGE, data);
        } else { 
            throw new Exception("Worker may not have destination for management messages!");
        }
    }  
    
    public void sendManagementMessage(Object data) throws Exception {
        if (!master) { 
            
            if (!getRegister()) { 
                throw new Exception("Worker not registered yet!");
            }
            
            sendManagementMessage(masterID, OPCODE_MESSAGE, data);
        } else { 
            throw new Exception("Master requires destination for management messages!");
        }
    }  
    
    public ReceivePort createDataReceivePort(String name) throws IOException {
        
        ReceivePort r = ibis.createReceivePort(portTypeOneToOne, name);
        
        receivePorts.put(name, r);
        
        r.enableConnections();
        
        return r;
    }
    
    public SendPort createDataSendPort(IbisIdentifier id, String target) throws IOException {
        
        SendPort s = ibis.createSendPort(portTypeOneToOne);
        
        synchronized (sendPorts) { 
            sendPorts.put(id, s);
        }

        s.connect(id, target, TIMEOUT,true);
        
        return s;
    }
    
    /*
    
    
    
    
    
    public Comm(String name, int connectionsIn, int connectionsOut) throws Exception { 

        ibis = IbisFactory.createIbis(ibisCapabilities, null, portTypeOneToOneIbis, 
                portTypeOneToOneIbis);

        receivePorts = new ReceivePort[connectionsIn];
        
        for (int i=0;i<connectionsIn;i++) { 
            receivePorts[i] = ibis.createReceivePort(portTypeOneToOneIbis, "in-" + i);
            receivePorts[i].enableConnections();
        }
                
        sendPorts = new SendPort[connectionsOut];
        
        for (int i=0;i<connectionsOut;i++) { 
            sendPorts[i] = ibis.createSendPort(portTypeOneToOneIbis);            
        }        
        
        IbisIdentifier tmp = ibis.registry().elect(name, TIMEOUT);
        
        if (!tmp.equals(ibis.identifier())) { 
            throw new Exception("My name is not unique!");
        }
        
        System.out.println("I am: " + name);
    }
        
    public void connect(int out, String target, int in) throws IOException {
        
        System.out.println("Connecting to: " + target + " on " + in);
        
        IbisIdentifier i = ibis.registry().getElectionResult(target, TIMEOUT);
        sendPorts[out].connect(i, "in-" + in, TIMEOUT, true);        
        
        System.out.println("Connecting OK");
    }
    
    public WriteMessage send(int out) throws IOException { 
        return sendPorts[out].newMessage();
    }

    public SendPort getSendPort(int i) {
        return sendPorts[i];
    }
    
    public ReadMessage receive(int in) throws IOException { 
        return receivePorts[in].receive();
    }
    
    public ReceivePort getReceivePort(int i) {
        return receivePorts[i];
    }
    */
    
    public void close() { 
        
        for (SendPort s : sendPorts.values()) { 
            try {
                s.close();
            } catch (IOException e) {
               // e.printStackTrace();
            }
        }
        
        for (ReceivePort r : receivePorts.values()) { 
            try {
                r.close(TIMEOUT);
            } catch (IOException e) {
                //e.printStackTrace();
            }
        }
        
        try {
            ibis.end();
        } catch (IOException e) {
            // TODO Auto-generated catch block
           // e.printStackTrace();
        }
    }
    
    private void register(IbisIdentifier id, Object data) { 
        
        if (!master) { 
            System.out.println("Worker got unexpected REGISTER message!");
            return;
        }
        
        managementCallback.register(id, data); 
    }
    
    private void registered(IbisIdentifier id, Object data) { 
        
        if (master) { 
            System.out.println("Master got unexpected REGISTERED message!");
            return;
        }
        
        managementCallback.registered(id, data); 
    }
   
    public void upcall(ReadMessage rm) throws IOException, ClassNotFoundException {
        
        IbisIdentifier sender = rm.origin().ibisIdentifier();
        
        byte opcode = rm.readByte();
        Object data = rm.readObject();
        rm.finish();
        
        switch (opcode) { 
        case OPCODE_REGISTER:
            register(sender, data);
            break;
        case OPCODE_REGISTERED:
            registered(sender, data);
            break;
        case OPCODE_MESSAGE:
            managementCallback.managementMessage(sender, data);
            break;
        default:
            System.out.println("Got unexpected OPCODE: " + opcode); 
        }
    }

    public void registerEndpoint(String name) throws Exception {
        
        IbisIdentifier tmp = ibis.registry().elect(name, TIMEOUT);

        if (!tmp.equals(ibis.identifier())) { 
            throw new Exception(name + " already in use!");
        }
    }

    public IbisIdentifier find(String name) throws IOException {
        return ibis.registry().getElectionResult(name, TIMEOUT);
    }

    public IbisIdentifier getLocalID() {
        return ibis.identifier();
    }

   
}



