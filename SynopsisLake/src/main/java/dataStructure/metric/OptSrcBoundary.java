package dataStructure.metric;

public class OptSrcBoundary {
	public int srcId;
	public double val;
	public double frequency;

	public OptSrcBoundary(int srcId, double val, double frequency) {
		this.srcId = srcId;
		this.val = val;
		this.frequency = frequency;
	}

	@Override
	public String toString() {
		return srcId + ": " + val;
	}
}
