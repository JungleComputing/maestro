package geneSequencing.sharedObjects;

import geneSequencing.Sequence;

import java.util.ArrayList;

/**
 * Shared data of the gene sequencing algorithm.
 * @author Kees van Reeuwijk
 *
 */
public class SharedData extends ibis.satin.SharedObject implements
        SharedDataInterface {

    private static final long serialVersionUID = -5286386054437063114L;

    private ArrayList<Sequence> querySequences;

    private ArrayList<Sequence> databaseSequences;

    public SharedData(ArrayList<Sequence> querySequences, ArrayList<Sequence> databaseSequences) {
        this.querySequences = querySequences;
        this.databaseSequences = databaseSequences;
    }

    /**
     * @return the databaseSequences
     */
    public ArrayList<Sequence> getDatabaseSequences() {
        return databaseSequences;
    }

    /**
     * @return the querySequences
     */
    public ArrayList<Sequence> getQuerySequences() {
        return querySequences;
    }
}