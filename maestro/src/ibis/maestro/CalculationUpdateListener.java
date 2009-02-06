/**
 * 
 */
package ibis.maestro;

/**
 * @author Kees van Reeuwijk
 *
 */
interface CalculationUpdateListener {
    void handleValueChange();

    void withdrawVertex(CalculationVertex calculationVertex);
}