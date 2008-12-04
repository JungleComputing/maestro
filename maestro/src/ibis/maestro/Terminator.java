package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;

/**
 * A thread that only runs (if at all) on the maestro, and sends stop messages
 * to random non-maestro nodes. Used to test fault-tolerance.
 * 
 * @author Kees van Reeuwijk.
 */
class Terminator extends Thread {
    /** How many nodes should we stop? */
    private double terminationQuotum;
    private double nodeQuotum;
    private final ArrayList<IbisIdentifier> victims = new ArrayList<IbisIdentifier>();
    private int failedMessageCount = 0;
    private int messageCount = 0;
    private final long initialSleepTime; // Time in ms before the first stop
    // message.
    private final long sleepTime; // Time in ms between stop messages.

    Terminator(double startQuotum, double nodeQuotum, long initialSleepTime,
	    long sleepTime) {
	super("Maestro Terminator");
	this.terminationQuotum = startQuotum;
	this.nodeQuotum = nodeQuotum;
	this.initialSleepTime = initialSleepTime;
	this.sleepTime = sleepTime;
	setDaemon(true);
    }

    private void sendStopMessage(IbisIdentifier target) {
	long len;
	SendPort port = null;
	StopNodeMessage msg = new StopNodeMessage();
	if (Settings.traceTerminator) {
	    Globals.log.reportProgress("Sending stop message to " + target);
	}
	try {
	    port = Globals.localIbis.createSendPort(PacketSendPort.portType);
	    port.connect(target, Globals.receivePortName,
		    Settings.ESSENTIAL_COMMUNICATION_TIMEOUT, true);
	    WriteMessage writeMessage = port.newMessage();
	    try {
		writeMessage.writeObject(msg);
	    } finally {
		len = writeMessage.finish();
	    }
	    if (Settings.traceSends) {
		Globals.log.reportProgress("Sent stop message of " + len
			+ " bytes: " + msg);
	    }
	} catch (IOException e) {
	    failedMessageCount++;
	} finally {
	    try {
		if (port != null) {
		    port.close();
		}
	    } catch (Throwable x) {
		// Nothing we can do.
	    }
	}
	terminationQuotum--;
	messageCount++;
    }

    private boolean stopRandomNode() {
	IbisIdentifier victim;

	synchronized (this) {
	    if (victims.isEmpty()) {
		return false;
	    }
	    // Draw a random victim from the list.
	    int ix = Globals.rng.nextInt(victims.size());
	    victim = victims.remove(ix);
	}
	sendStopMessage(victim);
	return true;
    }

    /** Runs this thread. */
    @Override
    public void run() {
	long ourSleepTime = initialSleepTime;
	if (Settings.traceTerminator) {
	    Globals.log
		    .reportProgress("Starting terminator thread. initialSleepTime="
			    + initialSleepTime
			    + " ms, sleepTime="
			    + sleepTime
			    + " ms");
	}
	while (true) {
	    long deadline = System.currentTimeMillis() + ourSleepTime;
	    long now;

	    do {
		long waitTime = deadline - System.currentTimeMillis();
		try {
		    if (Settings.traceTerminator) {
			Globals.log.reportProgress("Terminator: waiting "
				+ waitTime + " ms; terminationQuotum="
				+ terminationQuotum);
		    }
		    synchronized (this) {
			this.wait(waitTime);
		    }
		} catch (InterruptedException e) {
		    // ignore.
		}
		now = System.currentTimeMillis();
	    } while (now < deadline);
	    if (terminationQuotum >= 1) {
		stopRandomNode();
	    }
	    ourSleepTime = sleepTime;
	}
    }

    synchronized void registerNode(IbisIdentifier ibis) {
	if (!ibis.equals(Globals.localIbis.identifier())) {
	    victims.add(ibis);
	}
	terminationQuotum += nodeQuotum;
    }

    synchronized void removeNode(IbisIdentifier ibis) {
	victims.remove(ibis);
    }

    void printStatistics(PrintStream s) {
	s.println("Sent " + messageCount + " stop messages, with "
		+ failedMessageCount + " failures");
    }

}
