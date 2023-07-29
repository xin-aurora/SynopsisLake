package dataStructure.objFunction;

public class SrcEndBoundary {
	public int srcId;
	public double val;

	public SrcEndBoundary(int srcId, double val) {
		this.srcId = srcId;
		this.val = val;
	}

	@Override
	public String toString() {
		return srcId + ": " + val;
	}
}
