package dataStructure;

public class RangeQuery1D {

	public double minLon;
	public double maxLon;
	public double ans;
	public RangeQuery1D(double minLon, double maxLon, double ans) {
		this.minLon = minLon;
		this.maxLon = maxLon;
		this.ans = ans;
	}

	
	public void UpdateAns(double newAns) {
		this.ans += newAns;
	}
	
	@Override
	public final String toString() {
		return minLon + "," + maxLon + "," + ans;
	}
	
//	public final String toString() {
//		return "" + ans;
//	}
}
