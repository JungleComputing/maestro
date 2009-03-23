package ibis.maestro;

public class ParallelJobContext {
	final RunJobMessage message;
	final double runMoment;

	ParallelJobContext(RunJobMessage message, double runMoment) {
		this.message = message;
		this.runMoment = runMoment;
	}

}
