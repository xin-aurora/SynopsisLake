package histogram;

import java.io.Serializable;

public class SpatialHistogram implements Serializable{

	protected double mCellArea = 0.0;
	
	// boundary 
	protected double mMinLon;
	protected double mMinLat;
	protected double mMaxLon;
	protected double mMaxLat;

	protected int mNumLonBucket;
	protected int mNumLatBucket;

	protected double mLonUnitBucket;
	protected double mLatUnitBucket;
	
	protected int mSelectivity;
	
	public SpatialHistogram(double minLon, double minLat, double maxLon, double maxLat, int numLonBucket,
			int numLatBucket) {
		
		mMinLon = minLon;
		mMinLat = minLat;
		mMaxLon = maxLon;
		mMaxLat = maxLat;

		mNumLonBucket = numLonBucket;
		mNumLatBucket = numLatBucket;

		mLonUnitBucket = (maxLon - minLon) / mNumLonBucket;
		mLatUnitBucket = (maxLat - minLat) / mNumLatBucket;

		// initialization
		mCellArea = mLonUnitBucket * mLatUnitBucket;
		
	}
	
	protected int[] getBId(double latitude, double longitude) {

		int[] point = new int[2];

		int xP = -1;
		int yP = -1;

		if (latitude <= mMinLat) {
			xP = 0;
		} else if (latitude >= mMaxLat) {
			xP = mNumLatBucket - 1;
		} else {
			xP = (int) ((latitude - mMinLat) / mLatUnitBucket);
		}
		if (longitude <= mMinLon) {
			yP = 0;
		} else if (longitude >= mMaxLon) {
			yP = mNumLonBucket - 1;
		} else {
			yP = (int) ((longitude - mMinLon) / mLonUnitBucket);
		}

		point[0] = xP;
		point[1] = yP;

		return point;
	}
	
	public double getLonUnit() {
		return mLonUnitBucket;
	}
	public double getLatUnit() {
		return mLatUnitBucket;
	}
	
	public int getNumLonBucket() {
		return mNumLonBucket;
	}
	
	public int getNumLatBucket() {
		return mNumLatBucket;
	}
	
	public int getTotalNumBuckets() {
		return mNumLonBucket * mNumLatBucket;
	}
	
}
