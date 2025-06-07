package dataStructure.objFunction;

public class Interval {
	public int id;
	public double low;
	public double high;
	public double data;
	public double density;

	public Interval(int bId, double start, double end) {
		this.id = bId;
		this.low = start;
		this.high = end;
		this.data = 0;
	}
	
	public void UpdateData(double updateData) {
		data += updateData;
		this.density = data / (high - low);
	}

	@Override
	public String toString() {
		return "[" + id + ": " + String.format( "%.3f", low) + "-" 
				+ String.format( "%.3f", high) + ": " + String.format( "%.3f", data) 
				+ ", " + density + "]";
	}
}
