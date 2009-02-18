package ibis.maestro;

/**
 * A simple synchronized up/down counter.
 * 
 * @author Kees van Reeuwijk.
 */
class UpDownCounter {
    private int value = 0;

    UpDownCounter(int i) {
        value = i;
    }

    synchronized void up() {
        value++;
        notifyAll();
    }

    synchronized void up(int i) {
        value += i;
        notifyAll();
    }

    synchronized void down() {
        value--;
        notifyAll();
    }

    private synchronized int get() {
        return value;
    }

    synchronized boolean isAbove(int v) {
        return this.value > v;
    }

    synchronized boolean isBelow(int v) {
        return this.value < v;
    }

    /**
     * Returns a string representation of this counter. (Overrides method in
     * superclass.)
     * 
     * @return The string representation.
     */
    @Override
    public String toString() {
        return Integer.toString(get());
    }
}
