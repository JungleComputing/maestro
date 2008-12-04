package ibis.maestro;

/**
 * A boolean with synchronous access.
 * 
 * @author Kees van Reeuwijk.
 */
class Flag {
    private boolean flag;

    Flag(boolean flag) {
	this.flag = flag;
    }

    synchronized void set() {
	flag = true;
    }

    synchronized void reset() {
	flag = false;
    }

    synchronized void set(boolean val) {
	flag = val;
    }

    synchronized boolean isSet() {
	return flag;
    }

    synchronized boolean getAndReset() {
	boolean res = flag;
	flag = false;
	return res;
    }
}
