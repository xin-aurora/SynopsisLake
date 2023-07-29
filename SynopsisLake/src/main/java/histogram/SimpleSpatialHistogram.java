package histogram;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

import dataStructure.CellCellOverlapInfo;
import dataStructure.SSH2DCell;

public class SimpleSpatialHistogram extends SpatialHistogram implements Serializable{

	// histogram cells
	private SSH2DCell[][] mCells;
	double standardDeviation = 0;
	public int totalN = 0;
	
	public ArrayList<ArrayList<Double>> points; // 0: lon, 1: lat

	public SimpleSpatialHistogram(double minLon, double minLat, double maxLon, double maxLat, int numLonBucket,
			int numLatBucket) {
		super(minLon, minLat, maxLon, maxLat, numLonBucket, numLatBucket);

		mCells = new SSH2DCell[numLonBucket][numLatBucket];
		ArrayList<Double> lon = new ArrayList<Double>();
		ArrayList<Double> lat = new ArrayList<Double>();
		boolean fillLat = false;
		for (int i = 0; i < numLonBucket; i++) {
			lon.add(mMinLon + i * mLonUnitBucket);
			for (int j = 0; j < numLatBucket; j++) {
				double cellMinLon = mMinLon + i * mLonUnitBucket;
				double cellMaxLon = cellMinLon + mLonUnitBucket;
				double cellMinLat = mMinLat + j * mLatUnitBucket;
				double cellMaxLat = cellMinLat + mLatUnitBucket;
				mCells[i][j] = new SSH2DCell(cellMinLon, cellMinLat, cellMaxLon, cellMaxLat);
				if (!fillLat) {
					lat.add(cellMinLat);
				}
			}
			fillLat = true;
		}
		lon.add(maxLon);
		lat.add(maxLat);
		points = new ArrayList<ArrayList<Double>>();
		points.add(lon);
		points.add(lat);
	}
	
	public SimpleSpatialHistogram(double minLon, double minLat, double maxLon, double maxLat,int numLonBucket,
			int numLatBucket, double[] lons, double[] lats) {
		super(minLon, minLat, maxLon, maxLat, numLonBucket, numLatBucket);

		mCells = new SSH2DCell[numLonBucket][numLatBucket];
		double cellMinLon = lons[0];
		for (int i = 1; i < lons.length; i++) {
			double cellMaxLon = lons[i];
			double cellMinLat = lats[0];
			for (int j = 1; j < lats.length; j++) {
//				System.out.println("i=" + i + ", j=" + j);
				double cellMaxLat = lats[j];
				mCells[i-1][j-1] = new SSH2DCell(cellMinLon, cellMinLat, cellMaxLon, cellMaxLat);
				cellMinLat = cellMaxLat;
			}
			cellMinLon = cellMaxLon;
		}
		
	}


	public void addRecord(double lon, double lat) {

		if (isOverlapPointCell(mMinLon, mMinLat, mMaxLon, mMaxLat, lon, lat)) {
			int[] p = getBId(lat, lon);

			// p[0] lat, p[1]: lon
			mCells[p[1]][p[0]].update(1);
			totalN++;
		}

	}
	
	public void addRecord(double lon, double lat, boolean bl) {

		if (isOverlapPointCell(mMinLon, mMinLat, mMaxLon, mMaxLat, lon, lat)) {
			
//			for (int i = 0; i < mCells.length; i++) {
//				for (int j = 0; j < mCells[0].length; j++) {
//					SSH2DCell cell = mCells[i][j];
//					if (UtilsFunction.isOverlapPointCell(cell.minLon, cell.minLat, 
//							cell.maxLon, cell.maxLat, lon, lat)) {
//						mCells[i][j].update(1);
//						totalN++;
//						return;
//					}
//					
//				}
//			}
			
			int idxLon = 0;
			SSH2DCell cellLon = mCells[idxLon][0];
			while (cellLon.maxLon < lon) {
				idxLon++;
				cellLon = mCells[idxLon][0];
			}
			for (int j = 0; j < mCells[0].length; j++) {
				SSH2DCell cell = mCells[idxLon][j];
				if (isOverlapPointCell(cell.minLon, cell.minLat, 
						cell.maxLon, cell.maxLat, lon, lat)) {
					mCells[idxLon][j].update(1);
					totalN++;
					return;
				}
			}
		}

	}
	
	public void mergeHistogramCells(SSH2DCell[][] src) {
		int numLonBucketMerged = this.mNumLonBucket;
		int numLatBucketMerged = this.mNumLatBucket;

		int numLonBucketSrc = src.length;
		int numLatBucketSrc = src[0].length;

		int mDimension = 0;

		while (mDimension < numLonBucketMerged) {

			int srcDimension = 0;
			int mId = 0;
			int srcId = 0;
			while (srcDimension < numLonBucketSrc) {
				// getOverlap
				SSH2DCell mergedCell = mCells[mDimension][mId];
				SSH2DCell srcCell = src[srcDimension][srcId];
				CellCellOverlapInfo overlapInfo = getCellCellOverlapInfo(mergedCell.minLon, mergedCell.minLat,
						mergedCell.maxLon, mergedCell.maxLat, (mergedCell.maxLon - mergedCell.minLon),
						(mergedCell.maxLat - mergedCell.minLat), srcCell.minLon, srcCell.minLat, srcCell.maxLon,
						srcCell.maxLat, (srcCell.maxLon - srcCell.minLon), (srcCell.maxLat - srcCell.minLat));

				if (overlapInfo.overlap) {
					double c = src[srcDimension][srcId].getVal() * overlapInfo.mComAreaSource;
//					System.out.println("map " + src[srcDimension][srcId].getVal() + " to "  + srcDimension + "-" + srcId + " + " + c);
//					System.out.println("merge cell = " + mergedCell.minLon + "-" + mergedCell.maxLon + ", " +
//							 mergedCell.minLat + "-" + mergedCell.maxLat + ", " +
//								", src cell = " + srcCell.minLon + "-" + srcCell.maxLon + ", " +
//								srcCell.minLat + "-" + srcCell.maxLat);
//					System.out.println(srcDimension + "-" + srcId + " + " + overlapInfo);
					mCells[mDimension][mId].update(c);
				}
				if (srcCell.maxLat < mergedCell.maxLat) {
					srcId++;
				} else {
					mId++;
				}

				if (mId >= numLatBucketMerged || srcId >= numLatBucketSrc) {
					mId = 0;
					srcId = 0;
					srcDimension++;
				}
			}

			mDimension++;
		}
	}

	public void mergeHistogram(SimpleSpatialHistogram src) {
		int numLonBucketMerged = this.mNumLonBucket;
		int numLatBucketMerged = this.mNumLatBucket;

		int numLonBucketSrc = src.getNumLonBucket();
		int numLatBucketSrc = src.getNumLatBucket();

		int mDimension = 0;

		while (mDimension < numLonBucketMerged) {

			int srcDimension = 0;
			int mId = 0;
			int srcId = 0;
			while (srcDimension < numLonBucketSrc) {
				// getOverlap
				SSH2DCell mergedCell = mCells[mDimension][mId];
				SSH2DCell srcCell = src.getCellById(srcDimension, srcId);
				CellCellOverlapInfo overlapInfo = getCellCellOverlapInfo(mergedCell.minLon, mergedCell.minLat,
						mergedCell.maxLon, mergedCell.maxLat, (mergedCell.maxLon - mergedCell.minLon),
						(mergedCell.maxLat - mergedCell.minLat), srcCell.minLon, srcCell.minLat, srcCell.maxLon,
						srcCell.maxLat, (srcCell.maxLon - srcCell.minLon), (srcCell.maxLat - srcCell.minLat));

				if (overlapInfo.overlap) {
//					System.out.println(overlapInfo);
					double c = src.getCellById(srcDimension, srcId).getVal() * overlapInfo.mComAreaSource;
//					System.out.println(srcDimension + "-" + srcId + " + " + c);
					mCells[mDimension][mId].update(c);
				}
				if (srcCell.maxLat < mergedCell.maxLat) {
					srcId++;
				} else {
					mId++;
				}

				if (mId >= numLatBucketMerged || srcId >= numLatBucketSrc) {
					mId = 0;
					srcId = 0;
					srcDimension++;
				}
			}

			mDimension++;
		}
	}

	public SSH2DCell[][] getCells() {
		return mCells;
	}
	
	public SSH2DCell[] getCellsOneArray() {
		SSH2DCell[] cells = new SSH2DCell[mCells.length * mCells[0].length];
		int idx = 0;
//		System.out.println("cell boundary = " + mCells.length + ", " + mCells[0].length);
		for (int i=0; i<mCells.length; i++) {
			for (int j=0; j<mCells[0].length; j++) {
//				System.out.println("i=" + i + ", j=" + j);
				cells[idx] = mCells[i][j];
				idx++;
			}
		}
		return cells;
	}

	public SSH2DCell getCellById(int lon, int lat) {
		return mCells[lon][lat];
	}

	public int[] getArrayByIdx(int dimension) {
		int[] array = null;
		if (dimension == 0) {
			array = new int[mNumLonBucket];
		} else {
			array = new int[mNumLatBucket];
		}
		return array;
	}

	public double[] getBoundary(int cellIdx, boolean lon) {
		double[] boundary = new double[2];

		if (lon) {
			boundary[0] = mMinLon + cellIdx * mLonUnitBucket;
			boundary[1] = boundary[0] + mLonUnitBucket;
		} else {
			boundary[0] = mMinLat + cellIdx * mLatUnitBucket;
			boundary[1] = boundary[2] + mLatUnitBucket;
		}

		return boundary;
	}

	private CellCellOverlapInfo getCellCellOverlapInfo(double tMinLon, double tMinLat, double tMaxLon, double tMaxLat,
			double tLonUnit, double tLatUnit, double sMinLon, double sMinLat, double sMaxLon, double sMaxLat,
			double sLonUnit, double sLatUnit) {

		return new CellCellOverlapInfo(tMinLon, tMinLat, tMaxLon, tMaxLat, tLonUnit, tLatUnit, sMinLon, sMinLat,
				sMaxLon, sMaxLat, sLonUnit, sLatUnit);
	}
	
	public double computeStandardDeviation() {
		
		if (totalN == 0) {
			for (int i=0; i<mCells.length; i++) {
				for (int j=0; j<mCells[0].length; j++) {
					totalN += 1;
				}

			}
		}
		
		double mean = totalN / mNumLonBucket / mNumLatBucket;
		standardDeviation = 0;
		
//		System.out.println("totalN = " + totalN + ", mean = " + mean);
		
		for (int i=0; i<mCells.length; i++) {
			for (int j=0; j<mCells[0].length; j++) {
				standardDeviation += Math.pow(mCells[i][j].getVal() - mean, 2);
			}

		}
		
//		System.out.println("tmp = " + standardDeviation);
		standardDeviation = standardDeviation / mNumLatBucket / mNumLatBucket;
		
		standardDeviation = Math.sqrt(standardDeviation);
		
		return standardDeviation;
	}
	
	public static boolean isOverlapPointCell(double tMinLon, double tMinLat, double tMaxLon, double tMaxLat, double lon,
			double lat) {

		if (tMaxLon < lon || lon < tMinLon || tMaxLat < lat || lat < tMinLat) {
			return false;
		}

		return true;

	}
	
	public static SimpleSpatialHistogram load(String path, int lon, int lat, String mRegex) {

		SimpleSpatialHistogram histogram = null;
		File file = new File(path);

		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));

			String[] header = reader.readLine().split(",");
			double minLon = 0;
			double maxLon = 0;
			double minLat = 0;
			double maxLat = 0;
			if (header.length > 2) {
				minLon = Double.parseDouble(header[0].split(":")[1].trim());
				maxLon = Double.parseDouble(header[1].split(":")[1].trim());
				minLat = Double.parseDouble(header[2].split(":")[1].trim());
				maxLat = Double.parseDouble(header[3].split(":")[1].trim());
			} else {
				minLon = Double.parseDouble(header[0]);
				maxLon = Double.parseDouble(header[1]);
				minLat = Double.parseDouble(header[0]);
				maxLat = Double.parseDouble(header[1]);
			}
			histogram = new SimpleSpatialHistogram(minLon, minLat, maxLon, maxLat, lon, lat);
			String line = reader.readLine();
			while (line != null) {
				String[] tem = line.split(mRegex);

				histogram.addRecord(Double.parseDouble(tem[0]), Double.parseDouble(tem[1]));

				line = reader.readLine();
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return histogram;
	}

}
