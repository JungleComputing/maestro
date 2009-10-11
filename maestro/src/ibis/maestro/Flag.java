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

    void set() {
        set(true);
    }

    private synchronized void set(boolean val) {
        final boolean changed = flag != val;
        flag = val;
        if (changed) {
            this.notifyAll();
        }
    }

    synchronized boolean isSet() {
        return flag;
    }

    synchronized boolean getAndReset() {
        final boolean res = flag;
        flag = false;
        if (res) {
            this.notifyAll();
        }
        return res;
    }

    void waitUntilSet() {
        while (true) {
            synchronized (this) {
                if (flag) {
                    return;
                }

                try {
                    this.wait();
                } catch (final InterruptedException e) {
                    // Ignore.
                }
            }
        }
    }
}
