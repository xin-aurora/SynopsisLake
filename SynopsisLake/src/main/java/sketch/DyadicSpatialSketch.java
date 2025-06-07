package sketch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import histogram.GeometricHistogramOpt;
import utils.ByteConvertLibrary;
import utils.UtilsFunctionSketch;

/**
 * Partition domain N into 2^h intervals, each level 0 <= i <= h, partition N
 * into 2^(h-i) intervals, the 2^(h-i) intervals are equal-size interval, where
 * each interval has size = 2^i
 */
public class DyadicSpatialSketch {

	double min;
	double max;
	int h; // numOfLevel
	int numOfPointIntervals;
	double level0Unit;
	/**
	 * there are in total h+1 levels, 0, ..., h each levelSize consists of two
	 * number, levelSizes[level][0]: # of intervals in this level,
	 * levelSizes[level][1]: level size level i=0 we have point "intervals", level
	 * i=h we have a single interval covering the whole domain
	 */
	// # of intervals = # of counters
	ArrayList<int[]> intervalCounters;
	ArrayList<int[]> endPointCounters;

	public DyadicSpatialSketch(double min, double max, int numOfLevel) {
		this.min = min;
		this.max = max;
		this.h = numOfLevel;
		// initial the points for level-0
		this.numOfPointIntervals = (int) Math.pow(2, h);
		this.level0Unit = (max - min) / numOfPointIntervals;
		// initial counters
		int counter = 0;
		this.endPointCounters = new ArrayList<int[]>(numOfLevel);
		this.intervalCounters = new ArrayList<int[]>(numOfLevel);
		for (int level = 0; level <= numOfLevel; level++) {
			int numOfIntervals = (int) Math.pow(2, numOfLevel - level);
			// # of intervals = # of counters
			int[] pCounters = new int[numOfIntervals];
			endPointCounters.add(pCounters);
			int[] iCounters = new int[numOfIntervals];
			intervalCounters.add(iCounters);
			counter += numOfIntervals;
//			System.out.println("level-" + level + ", # of counters = " + numOfIntervals);
		}
//		System.out.println("unit = " + this.level0Unit);
//		System.out.println("total # of level = " + h + ", total # of counters = " + counter);
	}

	/**
	 * 
	 * @param l: lower endpoint
	 * @param u: upper endpoint compute D([l,u]): the smallest set of dyadic
	 *           intervals compute D([l]) dyadic point cover: all dyadic intervals
	 */
	public void computeDyadicCover(double l, double u) {
//		System.out.println("intervals = [" + l + "," + u + "].");
		int[] lPointCovers = computeDyadicPointCover(l);
		int[] uPointCovers = computeDyadicPointCover(u);
//		System.out.println("l point covers = " + Arrays.toString(lPointCovers));
//		System.out.println("r point covers = " + Arrays.toString(uPointCovers));
		computeDyadicCover(lPointCovers, uPointCovers);
//		System.out.println("point");
//		for (int i = 0; i < endPointCounters.size(); i++) {
//			System.out.println("level-" + i + ", counters = " + Arrays.toString(endPointCounters.get(i)));
//		}
//		System.out.println("interval");
//		for (int i = 0; i < intervalCounters.size(); i++) {
//			System.out.println("level-" + i + ", counters = " + Arrays.toString(intervalCounters.get(i)));
//		}
	}

	/**
	 * @param p: a value
	 * @return an array of length h+1, covered dyadic intervals, dyadicIntervals[i]:
	 *         the covered intervals in level i. When 0 < i < h, the interval
	 *         boundary is [l, u)
	 */
	private int[] computeDyadicPointCover(double p) {
		int[] dyadicIntervals = new int[h + 1];
		dyadicIntervals[h] = 0;
		// interval point in level 0
		// map input val to boundary point
		int level0Idx = (int) (Math.abs(p - min) / level0Unit);
		if (level0Idx == numOfPointIntervals) {
			level0Idx--;
		}
		dyadicIntervals[0] = level0Idx;
		// first level
		endPointCounters.get(0)[level0Idx]++;
		// last level
		endPointCounters.get(h)[0]++;
		// other levels
		for (int level = 1; level < h; level++) {
			int idx = level0Idx / (int) (Math.pow(2, level));
			dyadicIntervals[level] = idx;
			endPointCounters.get(level)[idx]++;
		}
		return dyadicIntervals;
	}

	/**
	 * find the smallest set of dyadic intervals to cover the interval [l,u] start
	 * start point: lCover[0], end point: uCover[0] at level i, the interval size =
	 * 2^i
	 * 
	 * @param lCover
	 * @param uCover
	 */
	private void computeDyadicCover(int[] lPointCovers, int[] uPointCovers) {
		int lPointIdx = lPointCovers[0];
		int uPointIdx = uPointCovers[0];
//		System.out.println("lower idx = " + lPointIdx + ", upper idx = " + uPointIdx);

		double tmpStart = Math.log(lPointIdx) / Math.log(2);
		double tmpEnd = Math.log(uPointIdx) / Math.log(2);

		// start from level x
		int x = (int) Math.ceil(tmpStart);
		// maxScale
		int y = (int) Math.floor(tmpEnd);
//		System.out.println("x=" + x + ", y=" + y);

		// start from max -> min
		// scale = y, until scale = 1
		// tmpStart -> lPointIdx
		// tmpEnd -> tmpStart
		// uPointIdx -> tmpEnd

		// lPointIdx -> 2^x
		int scale = y;
		int start = lPointIdx;
		int end = (int) Math.pow(2, x);
//		System.out.println("left: start=" + start + ", end=" + end);
		while (start < end) {
			int maxLen = (int) Math.pow(2, scale);
			if ((start + maxLen) <= end) {
//				System.out.println("maxLen=" + maxLen);
//				System.out.println("left: start=" + start + ", end=" + end);
				// update intervalCounters at 2^scale level
				int level = scale;
				int idx = end / maxLen - 1;
//				System.out.println("level-" + level + ", idx=" + idx);
				intervalCounters.get(level)[idx]++;
				end -= maxLen;
			} else {
				scale--;
			}
		}

		scale = y;
		end = (int) Math.pow(2, y);
		start = (int) Math.pow(2, x);
//		System.out.println("middle: start=" + start + ", end=" + end);
		while (start < end) {
			int maxLen = (int) Math.pow(2, scale);
			if ((start + maxLen) <= end) {
//				System.out.println("maxLen=" + maxLen);
//				System.out.println("middle: start=" + start + ", end=" + end);
				// update intervalCounters at 2^scale level
				int level = scale;
				int idx = end / maxLen - 1;
//				System.out.println("level-" + level + ", idx=" + idx);
				intervalCounters.get(level)[idx]++;
				end -= maxLen;
			} else {
				scale--;
			}
		}

		scale = y;
		start = (int) Math.pow(2, y);
		end = uPointIdx;
//		System.out.println("right: start=" + start + ", end=" + end);
		while (start < end) {
			int maxLen = (int) Math.pow(2, scale);
			if ((start + maxLen) <= end) {
//				System.out.println("maxLen=" + maxLen);
//				System.out.println("right: start=" + start + ", end=" + end);
				// update intervalCounters at 2^scale level
				int level = scale;
				int idx = start / maxLen;
//				System.out.println("level-" + level + ", idx=" + idx);
				intervalCounters.get(level)[idx]++;
				start += maxLen;
			} else {
				scale--;
			}
		}

		return;
	}

	public ArrayList<int[]> getIntervalsCoints() {
		return this.intervalCounters;
	}

	public ArrayList<int[]> getPointCounters() {
		return this.endPointCounters;
	}

	public static int computeH(int total) {
		if (total < 0) {
			throw new IllegalArgumentException("Total must be non-negative");
		}
		double power = Math.log(total + 2) / Math.log(2); // log base 2
		int h = (int) Math.round(power - 2);
		return h;
	}

	public DyadicSpatialSketch loadHistByDataArray(DyadicSpatialSketch sketch, double[][] dataArray, int idxMin,
			int idxMax) {

		for (int i = 0; i < dataArray.length; i++) {
			sketch.computeDyadicCover(dataArray[i][idxMin], dataArray[i][idxMax]);
		}

		return sketch;

	}

	public static void main(String[] args) {
		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/" + "Research/Spatial/sketches/new_submission/data/";

//		String ds1Path = folder + "bitSample.csv";
		String ds1Path = folder + "bitSample.csv";
		String ds2Path = folder + "uniformSample.csv";

//		minMinX=-0.009853320466660466, maxMaxX=1.0092839441844395
//		minMinY=-0.009957859625009289, maxMaxY=1.0091031706541236
		double min = -0.009853320466660466;
		double max = 1.0092839441844395;

//		int resolution = 15;
		int resolution = DyadicSpatialSketch.computeH(512*512);

		try {
			DyadicSpatialSketch ds1SketchX = new DyadicSpatialSketch(min, max, resolution);
			DyadicSpatialSketch ds1SketchY = new DyadicSpatialSketch(min, max, resolution);
			BufferedReader reader = new BufferedReader(new FileReader(ds1Path));
			String line = reader.readLine();
			while (line != null) {
				String[] tmp = line.split(",");
				double minX = Double.parseDouble(tmp[0]);
				double minY = Double.parseDouble(tmp[1]);
				double maxX = Double.parseDouble(tmp[2]);
				double maxY = Double.parseDouble(tmp[3]);
				line = reader.readLine();
				ds1SketchX.computeDyadicCover(minX, maxX);
				ds1SketchY.computeDyadicCover(minY, maxY);
			}
			reader.close();
			DyadicSpatialSketch ds2SketchX = new DyadicSpatialSketch(min, max, resolution);
			DyadicSpatialSketch ds2SketchY = new DyadicSpatialSketch(min, max, resolution);
			reader = new BufferedReader(new FileReader(ds2Path));
			line = reader.readLine();
			while (line != null) {
				String[] tmp = line.split(",");
				double minX = Double.parseDouble(tmp[0]);
				double minY = Double.parseDouble(tmp[1]);
				double maxX = Double.parseDouble(tmp[2]);
				double maxY = Double.parseDouble(tmp[3]);
				line = reader.readLine();
				ds2SketchX.computeDyadicCover(minX, maxX);
				ds2SketchY.computeDyadicCover(minY, maxY);
			}
			reader.close();
			double selectEstMRB = UtilsFunctionSketch.DyadicSpatialSketchSelectEst(ds1SketchX, ds1SketchY, ds2SketchX,
					ds2SketchY);
			System.out.println();
			System.out.println("resolution = " + resolution);
			System.out.println("MRB - spatial selectivity est = " + selectEstMRB);

			double selectEst = UtilsFunctionSketch.DyadicSpatialSketchSelectEst(ds1SketchX, ds2SketchX);

			System.out.println();
			System.out.println("resolution = " + resolution);
			System.out.println("interval - spatial selectivity est = " + selectEst);

			double GT = 3895385.0;
			double error = Math.abs(GT - selectEst) / GT;
			System.out.println("error=" + error);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
