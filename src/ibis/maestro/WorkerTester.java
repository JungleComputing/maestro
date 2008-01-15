package ibis.maestro;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.ReceivePortIdentifier;
import ibis.server.Server;

import java.io.IOException;
import java.util.Properties;

/**
 * Tests the Worker class.
 * 
 * @author Kees van Reeuwijk
 *
 */
public final class WorkerTester {
    private static final IbisCapabilities ibisCapabilities = new IbisCapabilities( IbisCapabilities.ELECTIONS_STRICT );
    private static final int JOBCOUNT = 4;
    private static final long SLEEPDURATION = 1000l;

    private ReceivePortIdentifier startWorker( Ibis myIbis ) throws IOException {
        Worker worker = new Worker( myIbis );
        worker.run();
        return worker.getJobPort();
    }
    
    private class JobResultHandler implements PacketReceiveListener<JobResultMessage> {
        /**
         * Handles job request message <code>message</code>.
         * @param result The job request message.
         */
        @Override
        public void packetReceived(PacketUpcallReceivePort<JobResultMessage> p, JobResultMessage result) {
            System.out.println( "Received a job result " + result + " from " + p );
        }
    }

    private static class SleepJob implements Job {
        /** Contractual obligation. */
        private static final long serialVersionUID = 1462594936513813340L;
        private final long duration;
        
        SleepJob( long duration ){
            this.duration = duration;
        }
        
        @Override
        public String toString(){
            return "[sleep " + duration + "ms]";
        }

        public JobReturn run() {
            try {
                Thread.sleep( duration );
            } catch (InterruptedException e) {
                // Nothing.
            }
            return new VoidReturnValue();
        }

        public int compareTo(Job other) {
            return 0;
        }
    }
    
    @SuppressWarnings("synthetic-access")
    private void run() throws Exception {
        long id = 0l;
        PacketUpcallReceivePort<JobResultMessage> resultPort;

        // Create an ibis instance.
        Properties serverProperties = new Properties();
        //serverProperties.setProperty( "ibis.server.port", "12642" );
        Server ibisServer = new Server( serverProperties );
        String serveraddress = ibisServer.getLocalAddress();
        Properties ibisProperties = new Properties();
        ibisProperties.setProperty( "ibis.server.address", serveraddress );
        ibisProperties.setProperty( "ibis.pool.name", "XXXpoolname" );
        Ibis ibis = IbisFactory.createIbis(ibisCapabilities, ibisProperties, true, null, PacketSendPort.portType, PacketUpcallReceivePort.portType, PacketBlockingReceivePort.portType );

        resultPort = new PacketUpcallReceivePort<JobResultMessage>( ibis, "resultPort", new JobResultHandler() );
        resultPort.enable();
        
        PacketSendPort<MasterMessage> workSendPort = new PacketSendPort<MasterMessage>( ibis );
        ReceivePortIdentifier workPort = startWorker( ibis );

        for( int i=0; i<JOBCOUNT; i++ ){
            SleepJob job = new SleepJob( SLEEPDURATION );
            RunJobMessage msg = new RunJobMessage( job, id++, resultPort.identifier() );
            workSendPort.send(msg, workPort);
            
        }
        System.out.println( "Test program has ended" );
    }

    /**
     * Runs the program.
     * @param args The command-line parameters of this run.
     */
    public static void main(String[] args) {
        try {
            new WorkerTester().run();            
        }
        catch( Exception e ) {
            e.printStackTrace( System.err );
        }
    }

}
