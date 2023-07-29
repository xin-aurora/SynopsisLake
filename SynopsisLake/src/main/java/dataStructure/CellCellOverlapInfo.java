package dataStructure;

import java.io.Serializable;

public class CellCellOverlapInfo implements Serializable{

	public boolean overlap = false;
	public double mWo = 0.0;
	public double mHo = 0.0;
	public double moX1 = 0.0;
	public double moY1 = 0.0;
	public double mWs = 0.0;
	public double mHs = 0.0;
	public double mWm = 0.0;
	public double mHm = 0.0;
	public double msX1 = 0.0;
	public double msY1 = 0.0;
	public double mExpW = 0.0;
	public double mExpH = 0.0;
	public double mExpX1 = 0.0;
	public double mExpY1 = 0.0;
	
	public int mNumObj = 0;

	public double mComAreaSource = 0;
	public double mComAreaTarget = 0;
	public double mComHTarget = 0;
	public double mComVTarget = 0;
	
	// back flag
	public boolean backSrcDimension = false;
	
	public CellCellOverlapInfo(double tMinLon, double tMinLat, double tMaxLon, double tMaxLat, double tLonUnit,
			double tLatUnit, double sMinLon, double sMinLat, double sMaxLon, double sMaxLat, double sLonUnit,
			double sLatUnit) {
		if (tMaxLon <= sMinLon || sMaxLon <= tMinLon || tMaxLat <= sMinLat || sMaxLat <= tMinLat) {
			return;
		} else {

			double overMinLon = Math.max(tMinLon, sMinLon);
			double overMaxLon = Math.min(tMaxLon, sMaxLon);
			
			double overMinLat = Math.max(tMinLat, sMinLat);
			double overMaxLat =  Math.min(tMaxLat, sMaxLat);
			// compute values
//			mWo = Math.min(tMaxLon, sMaxLon) - Math.max(tMinLon, sMinLon);
			mWo = overMaxLon - overMinLon;
//			mHo = Math.min(tMaxLat, sMaxLat) - Math.max(tMinLat, sMinLat);
			mHo = overMaxLat - overMinLat;
			if (mWo < 1.1102230246251565E-10 || mHo < 1.1102230246251565E-10) {
				return;
			}
			
			if (sMinLon <= tMaxLon && tMaxLon < sMaxLon) {
				backSrcDimension = true;
			}

			overlap = true;
			
			// compute values
			double tmp = mWo * mHo;
			mComAreaSource = tmp / sLonUnit / sLatUnit;
			mComAreaTarget = tmp / tLonUnit / tLatUnit;
			mComHTarget = tmp / tLonUnit / sLatUnit;
			mComVTarget = tmp / tLatUnit / sLonUnit;
		}
	}

	@Override
	public final String toString() {
		return "overlap = " + overlap + ", mComAreaSource = " + mComAreaSource + ", mComAreaTarget " + mComAreaTarget
				+ ", mComHTarget = " + mComHTarget + ", mComVTarget = " + mComVTarget;
	}
}
