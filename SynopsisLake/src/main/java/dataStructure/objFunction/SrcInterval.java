package dataStructure.objFunction;

public class SrcInterval {
	public int id;
	public int srcHistId;
	public double low;
	public double high;
	public double data = 0;

	public SrcInterval(int bId, double start, double end, double data, int srcHistId) {
		this.id = bId;
		this.low = start;
		this.high = end;
		this.srcHistId = srcHistId;
		this.data = data;
	}

	@Override
	public String toString() {
		return "H-" + srcHistId + ": [" + id + ": " + String.format("%.3f", low) + "-" + String.format("%.3f", high)
				+ ": " + String.format("%.3f", data) + "]";
	}
}
