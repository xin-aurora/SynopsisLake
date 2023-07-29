package dataStructure;

import java.io.Serializable;

public class SSH2DCell implements Serializable{
	public double minLon;
	public double minLat;
	public double maxLon;
	public double maxLat;
	double val = 0;

	SSH2DCell() {

	}

	public SSH2DCell(double minLon, double minLat, double maxLon, double maxLat) {
		this.minLon = minLon;
		this.minLat = minLat;
		this.maxLon = maxLon;
		this.maxLat = maxLat;
	}

	public double getVal() {
		return val;
	}
	
	public void fillVal(double v) {
		val = v;
	}

	public void update(double v) {
		val += v;
	}

	public void accumulate(SHCell cell) {
		val += cell.val;
	}

	@Override
	public final String toString() {
		return String.format("%.2f", minLon) + "-" + String.format("%.2f", maxLon) + ", "
				+ String.format("%.2f", minLat) + "-" + String.format("%.2f", maxLat) + ": " + val;
	}
	
//	public final String toString() {
//		return "" + val;
//	}
}
