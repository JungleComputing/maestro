package ibis.maestro;

import ibis.ipl.Ibis;

import java.util.Random;

/**
 * Global variables.
 * 
 * @author Kees van Reeuwijk
 * 
 */
class Globals {
	/** The name of the receive port of a node. */
	static final String receivePortName = "receivePort";

	/** The logger. */
	final static Logger log = new Logger();

	/** A random-number generator. */
	static final Random rng = new Random();

	static TaskType[] supportedTaskTypes = null;

	static Ibis localIbis;

	static TaskType[] allTaskTypes = null;
}
