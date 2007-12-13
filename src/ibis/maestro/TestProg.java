package ibis.maestro;

import java.io.IOException;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class TestProg {
    PortType portType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA, PortType.RECEIVE_EXPLICIT, PortType.CONNECTION_ONE_TO_ONE );

    IbisCapabilities ibisCapabilities = new IbisCapabilities( IbisCapabilities.ELECTIONS_STRICT );
    
    private void server( Ibis myIbis ) throws IOException {
        // Create a receive port and enable connections.
        ReceivePort receiver = myIbis.createReceivePort( portType, "server" );
        receiver.enableConnections();
        
        // Read the message.
        ReadMessage r = receiver.receive();
        String s = r.readString();
        r.finish();
        System.out.println( "Server received: " + s );
        
        // Close receive port.
        receiver.close();
    }
    
    private void client( Ibis myIbis, IbisIdentifier server ) throws IOException {
        // Create aa send port for sending requests and connect.
        SendPort sender = myIbis.createSendPort( portType );
        sender.connect( server, "server" );
        
        // Send the message.
        WriteMessage w = sender.newMessage();
        w.writeString("Hi there");
        w.finish();
        
        // Close ports
        sender.close();
    }
    
    private void run() throws Exception {
        // Create an ibis instance.
        Ibis ibis = IbisFactory.createIbis( ibisCapabilities, null, portType );
        
        // Elect a server
        IbisIdentifier server = ibis.registry().elect("Server");
        
        // If I am the server, run server, else run client.
        if( server.equals( ibis.identifier())){
            server(ibis);
        }
        else {
            client(ibis,server);
        }
        
        ibis.end();
    }
    
    public static void main( String args[] ){
        try {
            new TestProg().run();            
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }
}
