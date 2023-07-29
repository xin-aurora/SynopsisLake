package samples;

import java.io.Serializable;

public class StratifiedSpatialSamples implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6977130579774719964L;

	// boundary
	protected double minLon;
	protected double minLat;
	protected double maxLon;
	protected double maxLat;

	protected double lonBucketLenUnit;
	protected double latBucketLenUnit;

	// data and boundary
	double[][] data;
	double[] lonBoundary;
	double[] latBoundary;

	public int numLonBucket;
	public int numLatBucket;

	public StratifiedSpatialSamples(double minLon, double minLat, double maxLon, double maxLat, int numLonBucket,
			int numLatBucket) {
		
		this.minLon = minLon;
		this.maxLon = maxLon;
		this.minLat = minLat;
		this.maxLat = maxLat;

		this.data = new double[numLonBucket][numLatBucket];
		this.lonBoundary = new double[numLonBucket + 1];
		this.latBoundary = new double[numLatBucket + 1];
		this.lonBucketLenUnit = (maxLon - minLon) / numLonBucket;
		this.latBucketLenUnit = (maxLat - minLat) / numLatBucket;
		
		for (int i = 0; i < numLonBucket; i++) {
			lonBoundary[i] = minLon + i * lonBucketLenUnit;
		}
		lonBoundary[numLonBucket] = maxLon;
		for (int i = 0; i < numLatBucket; i++) {
			latBoundary[i] = minLat + i * latBucketLenUnit;
		}
		latBoundary[numLatBucket] = maxLat;
		
		this.numLonBucket = numLonBucket;
		this.numLatBucket = numLatBucket;
	}

}
