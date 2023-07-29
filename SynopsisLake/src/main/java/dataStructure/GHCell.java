package dataStructure;

public class GHCell {

	protected int mNumC = 0;
	protected double mSumRatioArea = 0.0;
	protected double mSumRatioH = 0.0;
	protected double mSumRatioV = 0.0;

	// cell info
	public double mMinLon;
	public double mMaxLon;
	public double mMinLat;
	public double mMaxLat;

	public GHCell() {

	}

	public GHCell(double minLon, double minLat, double maxLon, double maxLat) {
		mMinLon = minLon;
		mMinLat = minLat;
		mMaxLon = maxLon;
		mMaxLat = maxLat;
	}

	public void addCornerPoint(int c) {
		mNumC += c;
	}

	public void addRatioArea(double o) {
		mSumRatioArea += o;
	}

	public void addRatioH(double h) {
		mSumRatioH += h;
	}

	public void addRatioV(double v) {
		mSumRatioV += v;
	}

	public void accumulate(GHCell cell) {
		mMinLon = cell.mMinLon;
		mMinLat = cell.mMinLat;
		mMaxLon = cell.mMaxLon;
		mMaxLat = cell.mMaxLat;
		mNumC += cell.getmNumC();
		mSumRatioArea += cell.getmSumRatioArea();
		mSumRatioH += cell.mSumRatioH;
		mSumRatioV += cell.mSumRatioV;
	}

	public int getmNumC() {
		return mNumC;
	}

	public double getmSumRatioArea() {
		return mSumRatioArea;
	}

	public double getmSumRatioH() {
		return mSumRatioH;
	}

	public double getmSumRatioV() {
		return mSumRatioV;
	}

	@Override
	public final String toString() {
		return "# of corner = " + mNumC + ", ratio of area = " + mSumRatioArea + ", ratio of horizontal = " + mSumRatioH
				+ ", ratio of vertical = " + mSumRatioV;
	}
}
