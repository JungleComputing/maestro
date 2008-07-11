package ibis.maestro;

/**
 * FIXME.
 *
 * @author Kees van Reeuwijk.
 */
public class TaskInfoOnMaster
{
    private double researchBudget = 10.0;

    /** Add the given value to the research budget.
     * @param d The value to add.
     */
    void addResearchBudget( double d )
    {
        researchBudget = Math.min( 10, researchBudget+d );
    }
    
    boolean canResearch()
    {
        return researchBudget>=1.0;
    }
    
    void useResearchBudget()
    {
        researchBudget = Math.max( 0.0, researchBudget-1.0 );
    }
    
    /**
     * Returns a string representation of this task info. (Overrides method in superclass.)
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        return String.format( "researchBudget=%2.3f", researchBudget );
    }
}
