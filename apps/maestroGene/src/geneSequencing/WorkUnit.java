package geneSequencing;

import java.util.ArrayList;

import neobio.alignment.ScoringScheme;

/**
 * Created on Nov 17, 2006
 */
public class WorkUnit implements java.io.Serializable {
    public final String alignmentAlgorithm;

    public final int scoresOrAlignments;

    public final ScoringScheme scoringScheme;

    public ArrayList<Sequence> querySequences;

    public ArrayList<Sequence> databaseSequences;

    public final int maxScores;

    public final int threshold;

    public WorkUnit(final String alignmentAlgorithm, final int scoresOrAlignments,
            final ScoringScheme scoringScheme, final ArrayList<Sequence> querySequences,
            final ArrayList<Sequence> databaseSequences, final int maxScores, final int threshold) {
        super();
        this.alignmentAlgorithm = alignmentAlgorithm;
        this.scoresOrAlignments = scoresOrAlignments;
        this.scoringScheme = scoringScheme;
        this.querySequences = querySequences;
        this.databaseSequences = databaseSequences;
        this.maxScores = maxScores;
        this.threshold = threshold;
    }

    public WorkUnit splitQuerySequences(final int begin, final int end) {
        final ArrayList<Sequence> newQuerySequences = new ArrayList<Sequence>();

        for (int i = begin; i < end; i++) {
            newQuerySequences.add(querySequences.get(i));
        }
        return new WorkUnit(alignmentAlgorithm, scoresOrAlignments,
                scoringScheme, newQuerySequences, databaseSequences, maxScores,
                threshold);
    }

    public WorkUnit splitDatabaseSequences(final int begin, final int end) {
        final ArrayList<Sequence> newDatabaseSequences = new ArrayList<Sequence>();

        for (int i = begin; i < end; i++) {
            newDatabaseSequences.add(databaseSequences.get(i));
        }
        return new WorkUnit(alignmentAlgorithm, scoresOrAlignments,
                scoringScheme, querySequences, newDatabaseSequences, maxScores,
                threshold);
    }
    
    public ArrayList<WorkUnit> generateWorkUnits(final int threshold) {
        final ArrayList<WorkUnit> res = new ArrayList<WorkUnit>();
        
        int queryParts = querySequences.size() / threshold;
        if(querySequences.size() % threshold > 0) {
            queryParts++;
        }
        
        int databaseParts = databaseSequences.size() / threshold;
        if(databaseSequences.size() % threshold > 0) {
            databaseParts++;
        }

        for (int i = 0; i < queryParts; i++) {
            for (int j = 0; j < databaseParts; j++) {
                final ArrayList<Sequence> newQuerySequences = new ArrayList<Sequence>();
                final ArrayList<Sequence> newDatabaseSequences = new ArrayList<Sequence>();

                for(int x=0;x<threshold;x++) {
                    final int pos = i * threshold + x;
                    if(pos >= querySequences.size()) continue;
                    newQuerySequences.add(querySequences.get(pos));
                }

                for(int x=0;x<threshold;x++) {
                    final int pos = j * threshold + x;
                    if(pos >= databaseSequences.size()) continue;
                    newDatabaseSequences.add(databaseSequences.get(pos));
                }
                
                res.add(new WorkUnit(alignmentAlgorithm, scoresOrAlignments,
                        scoringScheme, newQuerySequences, newDatabaseSequences, maxScores,
                        threshold));
            }
        }
        
        return res;
    }
    
    public String toString() {
        final String res = "workunit: alg = " + alignmentAlgorithm
        + " sOrA = " + scoresOrAlignments
        + " scheme = " + scoringScheme
        + " maxScores = " + maxScores
        + " threshold = " + threshold
        + " queries = " + querySequences.size()
        + " databases = " + databaseSequences.size();
        return res;
   }
}
 