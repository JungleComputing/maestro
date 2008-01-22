package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisFactory;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;
import ibis.server.Server;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

/**
 * Small test program.
 * @author Kees van Reeuwijk
 *
 */
public class TestPorts {
    static final IbisCapabilities ibisCapabilities = new IbisCapabilities();
    static final PortType portType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_OBJECT, PortType.CONNECTION_MANY_TO_ONE, PortType.RECEIVE_AUTO_UPCALLS, PortType.RECEIVE_EXPLICIT );
    static final String receivePortName = "receivePort";
    Sender sender;

    private static class Message implements Serializable {
        private static final long serialVersionUID = 1L;
        String payload;
        
        Message( String payload ){
            this.payload = payload;
        }
        
        /**
         * Returns a string representation of this message.
         * @return The string representation.
         */
        @Override
        public String toString()
        {
            return "Message [" + payload + ']';
        }
    }

    private class Listener implements MessageUpcall {

        @Override
        public void upcall(ReadMessage arg0) {
            System.out.println( "Received message " + arg0 );            
        }
    }

    private static class Sender extends Thread {
        private final ReceivePortIdentifier receiver;
        private final Ibis ibis;

        Sender( Ibis ibis, ReceivePortIdentifier receiver ) {
            this.receiver = receiver;
            this.ibis = ibis;
        }
        
        @Override
        public void run(){
            System.out.println( "Started sender" );
            
            while( true ){
                Message m = new Message( "test" );
                try {
                    System.out.println( "Sending message " + m ); 
                    SendPort port = ibis.createSendPort(portType);
                    port.connect(receiver, 10000l, true );
                    WriteMessage msg = port.newMessage();
                    msg.writeObject( m );
                    msg.finish();
                    port.close();
                } catch (IOException e) {
                    System.out.println( "Cannot send message" );
                    e.printStackTrace();
                }
            }
        }
    }

    @SuppressWarnings("synthetic-access")
    private void run() throws Exception {
        // Create an ibis instance.
        Properties serverProperties = new Properties();
        //serverProperties.setProperty( "ibis.server.port", "12642" );
        Server ibisServer = new Server( serverProperties );
        String serveraddress = ibisServer.getLocalAddress();
        Ibis ibis = createMaestroIbis( serveraddress );

        Listener listener = new Listener();
        ReceivePort receiver = ibis.createReceivePort( portType, receivePortName, listener );
        //receiver.enableConnections();
        //receiver.enableMessageUpcalls();
        sender = new Sender( ibis, receiver.identifier() );
        sender.start();

        Thread.sleep( 1000000 );
        System.out.println( "Test program has ended" );
    }

    private Ibis createMaestroIbis( String serveraddress )
	    throws IbisCreationFailedException {
	Ibis ibis;
	Properties ibisProperties = new Properties();
        ibisProperties.setProperty( "ibis.server.address", serveraddress );
        ibisProperties.setProperty( "ibis.pool.name", "TestprogPool" );
        ibis = IbisFactory.createIbis(
            ibisCapabilities,
            ibisProperties,
            true,
            null,
            PacketSendPort.portType,
            PacketUpcallReceivePort.portType,
            PacketBlockingReceivePort.portType
        );
	return ibis;
    }

    /** The command-line interface of this program.
     * 
     * @param args The list of command-line parameters.
     */
    public static void main( String args[] ) {
	System.out.println( "Running on platform " + Service.getPlatformVersion() );
	try {
            new TestPorts().run();            
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }
}
