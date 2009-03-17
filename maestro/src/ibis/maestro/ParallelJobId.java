/**
 * 
 */
package ibis.maestro;

class ParallelJobId {
    private static int nextId = 0;

    final int id;

    ParallelJobId( int n ){
        this.id = n;
    }

    synchronized static ParallelJobId getNext()
    {
        return new ParallelJobId( nextId++ );
    }
}