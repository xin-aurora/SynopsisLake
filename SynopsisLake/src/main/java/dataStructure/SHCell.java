package dataStructure;

public class SHCell {

	double min;
	double max;
	double val = 0;

	public SHCell() {

	}

	public SHCell(double min, double max) {
		this.min = min;
		this.max = max;
	}

	public double getMin() {
		return min;
	}

	public double getMax() {
		return max;
	}

	public double getVal() {
		return val;
	}

	public void update(double v) {
		val += v;
	}

	public void accumulate(SHCell cell) {
		val += cell.val;
	}
	
	public boolean isOverlap(SHCell cell) {
		if (cell.max < min || max < cell.min) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public final String toString() {
		return String.format("%.2f", min) + "-" + String.format("%.2f", max) + ": " + val;
	}
}
