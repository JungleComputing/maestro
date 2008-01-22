package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisCreationFailedException;
import ibis.ipl.IbisFactory;
import ibis.ipl.ReceivePortIdentifier;
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
    IbisCapabilities ibisCapabilities = new IbisCapabilities( IbisCapabilities.ELECTIONS_STRICT );
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

    private class Listener implements PacketReceiveListener<Message> {
        public void packetReceived(PacketUpcallReceivePort<Message> p, Message packet) {
            System.out.println( "Received message " + p + " from port " + p );
        }
    }

    private static class Sender extends Thread {
        private PacketSendPort<Message> port;
        ReceivePortIdentifier receiver;

        Sender( Ibis ibis, ReceivePortIdentifier receiver ) throws IOException {
            port = new PacketSendPort<Message>( ibis );
            this.receiver = receiver;
        }
        
        @Override
        public void run(){
            System.out.println( "Started sender" );
            
            while( true ){
                Message m = new Message( "test" );
                try {
                    System.out.println( "Sending message " + m ); 
                    port.send(m, receiver);
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
        Ibis ibis;
        ibis = createMaestroIbis( serveraddress );

        Listener listener = new Listener();
        PacketUpcallReceivePort<Message> receiver = new PacketUpcallReceivePort<Message>( ibis, "link", listener );
        sender = new Sender( ibis, receiver.identifier() );
        sender.start();

        //ibis.end();
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
