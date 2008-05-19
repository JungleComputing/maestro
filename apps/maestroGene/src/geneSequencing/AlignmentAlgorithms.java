package geneSequencing;

/**
 * Names of possible alignment algorithms.
 * 
 * @author Kees van Reeuwijk
 *
 */
public interface AlignmentAlgorithms {
    /** */
    public final String SMITH_WATERMAN = "sw";

    /** */
    public final String NEEDLEMAN_WUNSCH = "nw";

    /** */
    public final String CROCHEMORE_GLOBAL = "cg";

    /** */
    public final String CROCHEMORE_LOCAL = "cl";
}
