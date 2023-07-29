package dataStructure;

public class RangeQuery2D {

	public double minLon;
	public double maxLon;
	public double minLat;
	public double maxLat;
	public double ans;
	public RangeQuery2D(double minLon, double maxLon, double minLat, double maxLat, double ans) {
		this.minLon = minLon;
		this.maxLon = maxLon;
		this.minLat = minLat;
		this.maxLat = maxLat;
		this.ans = ans;
	}
	
	public RangeQuery2D(double minLon, double maxLon, double ans) {
		this.minLon = minLon;
		this.maxLon = maxLon;
		this.minLat = minLon;
		this.maxLat = maxLon;
		this.ans = ans;
	}
	
	public void UpdateAns(double newAns) {
		this.ans += newAns;
	}
	
	@Override
	public final String toString() {
		return minLon + "," + minLat + "," + maxLon + ","+ maxLat + "," + ans + ",";
	}
	
//	public final String toString() {
//		return "" + ans;
//	}
}
