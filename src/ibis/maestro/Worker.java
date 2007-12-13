package ibis.maestro;

import java.io.IOException;

import ibis.ipl.Ibis;
import ibis.ipl.MessageUpcall;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

@SuppressWarnings("synthetic-access")
public class Worker {
    private static PortType jobPortType = new PortType( PortType.COMMUNICATION_RELIABLE, PortType.SERIALIZATION_DATA, PortType.RECEIVE_AUTO_UPCALLS, PortType.CONNECTION_MANY_TO_ONE );
    private ReceivePort jobPort;
    private SendPort jobRequestPort;
    private JobRequestHandler jobRequestHandler = new JobRequestHandler();

    private class JobRequestHandler implements MessageUpcall {

        public void upcall(ReadMessage message) throws IOException, ClassNotFoundException {
        }
        
    }
    
    private void sendWorkRequest() throws IOException
    {
        WriteMessage msg = jobRequestPort.newMessage();
        msg.writeObject( jobPort.identifier() );
    }

    public Worker( Ibis ibis ) throws IOException
    {
        jobPort = ibis.createReceivePort(jobPortType, "jobPort", jobRequestHandler );
    }
}
