package baseline;

public class NearOptimalBoundary {
	
	public int idx = -1;
	public double i = -1; // i is coordinates in the paper
	public double y = -1; // y is the frequency in the paper
	public NearOptimalBoundary(int idx, double i, double y) {
		this.idx = idx;
		this.i = i;
		this.y = y;
	}
	
	public void increaseFrequency(double newY) {
		this.y += newY;
	}
	
	@Override
	public String toString() {
		return idx + "-" + i + ":" + y;
	}
}
