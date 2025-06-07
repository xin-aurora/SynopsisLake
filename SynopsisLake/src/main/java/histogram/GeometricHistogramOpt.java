package histogram;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import dataStructure.GHCell;
import dataStructure.File.LoadFile;
import utils.UtilsFunction;
import utils.UtilsFunctionHistogram;

public class GeometricHistogramOpt implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8324026773263274273L;

	// boundary
	double minLon;
	double minLat;
	double maxLon;
	double maxLat;

	double lonBucketLenUnit;
	double latBucketLenUnit;

	// data and boundary
	double[][] count;
	double[][] mSumRatioArea;
	double[][] mSumRatioH;
	double[][] mSumRatioV;
	double[] lonBoundary;
	double[] latBoundary;

	public int numLonBucket;
	public int numLatBucket;

	public ArrayList<double[]> frequency = new ArrayList<double[]>();

	double standardDeviation = 0;
	public int totalN = 0;
	
	// alignment score
	double[][] targetAggTotal;

	public GeometricHistogramOpt(double minLon, double minLat, double maxLon, double maxLat, int numLonBucket,
			int numLatBucket) {
		this.minLon = minLon;
		this.maxLon = maxLon;
		this.minLat = minLat;
		this.maxLat = maxLat;

		this.count = new double[numLonBucket][numLatBucket];
		this.mSumRatioArea = new double[numLonBucket][numLatBucket];
		this.mSumRatioH = new double[numLonBucket][numLatBucket];
		this.mSumRatioV = new double[numLonBucket][numLatBucket];
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

		// frequency
		double[] lonFre = new double[numLonBucket + 1];
		double[] latFre = new double[numLatBucket + 1];
		frequency.add(lonFre);
		frequency.add(latFre);

		this.numLonBucket = numLonBucket;
		this.numLatBucket = numLatBucket;
	}
	
	public GeometricHistogramOpt(double minLon, double minLat, double maxLon, double maxLat, int numLonBucket,
			int numLatBucket, double[] lons, double[] lats) {

		this.minLon = minLon;
		this.maxLon = maxLon;
		this.minLat = minLat;
		this.maxLat = maxLat;

		this.count = new double[numLonBucket][numLatBucket];
		this.mSumRatioArea = new double[numLonBucket][numLatBucket];
		this.mSumRatioH = new double[numLonBucket][numLatBucket];
		this.mSumRatioV = new double[numLonBucket][numLatBucket];
		this.lonBoundary = lons;
		this.latBoundary = lats;

		// frequency
		double[] lonFre = new double[numLonBucket + 1];
		double[] latFre = new double[numLatBucket + 1];
		frequency.add(lonFre);
		frequency.add(latFre);

		this.numLonBucket = numLonBucket;
		this.numLatBucket = numLatBucket;
		
		targetAggTotal = new double[numLonBucket][numLatBucket];
	}
	
	public GeometricHistogramOpt(double minLon, double minLat, double maxLon, double maxLat, int numLonBucket,
			int numLatBucket, double[][] count, double[][] sumRatioArea, double[][] sumRatioH, 
			double[][] sumRatioV,
			double[] lons, double[] lats, double[] lonFre, double[] latFre,
			double sd) {
		this.minLon = minLon;
		this.maxLon = maxLon;
		this.minLat = minLat;
		this.maxLat = maxLat;

		this.count = count;
		this.mSumRatioArea = sumRatioArea;
		this.mSumRatioH = sumRatioH;
		this.mSumRatioV = sumRatioV;
		this.lonBoundary = lons;
		this.latBoundary = lats;
		frequency.add(lonFre);
		frequency.add(latFre);

		this.numLonBucket = numLonBucket;
		this.numLatBucket = numLatBucket;

		this.standardDeviation = sd;
	}
	
	
	/**
	 * GeometricHistogramOpt Header Synopsis size: d1 size, d2 size,
	 * standardDeviation
	 * 
	 */
	public String headerBuilder() {

		String header = count.length + "," + count[1].length + ",";
		double sd = computeStandardDeviation();
//		System.out.println("sd = " + sd);
		header += sd + "\n";

		return header;
	}
	
	public byte[] byteHeaderBuilder() {

//		String header = data.length + "," + data[1].length + ",";
//		double sd = computeStandardDeviation();
////		System.out.println("sd = " + sd);
//		header += sd + "\n";
		
		ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES*2 + Double.BYTES);
		byteBuffer.putInt(count.length);
		byteBuffer.putInt(4, count[0].length);
		double sd = computeStandardDeviation();
		byteBuffer.putDouble(8, sd);

		return byteBuffer.array();
	}

	public void addRecord(double minLon, double minLat, double maxLon, double maxLat) {

		// --> lon
		// P1 - P2
		// P3 - P4
		int[] p1 = getBId(minLat, minLon);
		int[] p2 = getBId(minLat, maxLon);
		int[] p3 = getBId(maxLat, minLon);
		
//		System.out.println("p1 = " + Arrays.toString(p1));
//		System.out.println("p2 = " + Arrays.toString(p2));
//		System.out.println("p3 = " + Arrays.toString(p3));

		for (int lat = p1[0]; lat <= p3[0]; lat++) {
			for (int lon = p1[1]; lon <= p2[1]; lon++) {
//				int idx = lon + lat * mNumLonBucket;
//				System.out.println("idx = " + idx);
//				System.out.println("lon = " + lon + ", lat = " + lat);
				double cellMinLon = lonBoundary[lon];
				double cellMaxLon = lonBoundary[lon + 1];
				double cellMinLat = latBoundary[lat];
				double cellMaxLat = latBoundary[lat + 1];

//				System.out.println("Cell: " + cellMinLon + "-" + cellMaxLon + ", " + cellMinLat + "-" + cellMaxLat);

				GHCell tmpCell = fillCell(minLon, minLat, maxLon, maxLat, cellMinLon, cellMinLat, cellMaxLon,
						cellMaxLat);
//				mCells[lat][lon].accumulate(tmpCell);
				if (tmpCell.getmSumRatioArea() > 0) {
					totalN += 1;
				}
				fillCell(tmpCell, lon, lat);
				frequency.get(0)[lon] += 1;
				frequency.get(1)[lat] += 1;
			}
		}
	}
	
	public void addRecordNonUnitform(double minLon, double minLat, double maxLon, double maxLat) {
		if (UtilsFunction.isOverlapCellCell(lonBoundary[0], latBoundary[0], lonBoundary[numLonBucket],
				latBoundary[numLatBucket], minLon, minLat, maxLon, maxLat)) {
			
			for (int cLonIdx = 0; cLonIdx < lonBoundary.length-1; cLonIdx++) {
				for (int cLatIdx = 0; cLatIdx < latBoundary.length-1; cLatIdx++) {
					double cellMinLon = lonBoundary[cLonIdx];
					double cellMaxLon = lonBoundary[cLonIdx + 1];
					double cellMinLat = latBoundary[cLatIdx];
					double cellMaxLat = latBoundary[cLatIdx + 1];
					
					if (UtilsFunction.isOverlapCellCell(cellMinLon, cellMaxLon, cellMinLat, cellMaxLat, 
							minLon, minLat, maxLon, maxLat)) {
						GHCell tmpCell = fillCellNonUniform(minLon, minLat, maxLon, maxLat, cellMinLon, cellMinLat, cellMaxLon,
								cellMaxLat);
//						mCells[lat][lon].accumulate(tmpCell);
						if (tmpCell.getmSumRatioArea() > 0) {
							totalN += 1;
						}
						fillCell(tmpCell, cLonIdx, cLatIdx);
						frequency.get(0)[cLonIdx] += 1;
						frequency.get(1)[cLatIdx] += 1;
					}
					
				}
			}
		} 
//		else {
//			System.out.println("out of range");
//		}
	}

	private void fillCell(GHCell cell, int lonIdx, int latIdx) {
		this.count[lonIdx][latIdx] += cell.getmNumC();
		this.mSumRatioArea[lonIdx][latIdx]  += cell.getmSumRatioArea();
		this.mSumRatioH[lonIdx][latIdx] += cell.getmSumRatioH();
		this.mSumRatioV[lonIdx][latIdx] += cell.getmSumRatioV();
	}

	private GHCell fillCell(double minLon, double minLat, double maxLon, double maxLat, double minLonCell,
			double minLatCell, double maxLonCell, double maxLatCell) {
		GHCell cell = new GHCell(minLonCell, minLatCell, maxLonCell, maxLatCell);

		double minLonInt = 0.0;
		double maxLonInt = 0.0;
		double minLatInt = 0.0;
		double maxLatInt = 0.0;

		if (minLon < minLonCell) {
			minLonInt = minLonCell;

			if (maxLonCell < maxLon) {
				maxLonInt = maxLonCell;
				if (minLat < minLatCell) {
					minLatInt = minLatCell;

					if (maxLatCell < maxLat) {
						// cell in object
						maxLatInt = maxLatCell;
					} else {
						// maxLat <= maxLatCell;
						maxLatInt = maxLat;
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit);
					}

				} else {
					// minLatCell <= minLat
					minLatInt = minLat;

					if (maxLatCell < maxLat) {
						maxLatInt = maxLatCell;
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit);
					} else {
						// maxLat <= maxLatCell;
						maxLatInt = maxLat;
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit * 2);
					}
				}

			} else {
				// !!! might intersection vertices
				// maxLon <= maxLonCell
				maxLonInt = maxLon;
				if (minLat < minLatCell) {
					minLatInt = minLatCell;
					if (maxLatCell < maxLat) {
						// No intersection vertices
						maxLatInt = maxLatCell;
					} else {
						// maxLat <= maxLatCell;
						maxLatInt = maxLat;
						cell.addCornerPoint(1);
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit);
					}
					cell.addRatioV((maxLatInt - minLatInt) / latBucketLenUnit);
				} else {
					// minLatCell <= minLat
					minLatInt = minLat;
					if (maxLatCell < maxLat) {
						maxLatInt = maxLatCell;
						cell.addCornerPoint(1);
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit);
					} else {
						// maxLat <= maxLatCell;
						maxLatInt = maxLat;
						cell.addCornerPoint(2);
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit * 2);
					}
					cell.addRatioV((maxLatInt - minLatInt) / latBucketLenUnit);
				}

			}

		} else {
			// minLonCell <= minLon
			// no HL
			minLonInt = minLon;
			if (maxLonCell < maxLon) {
				maxLonInt = maxLonCell;
				if (minLat < minLatCell) {
					minLatInt = minLatCell;
					if (maxLatCell < maxLat) {
						// No intersection vertices
						maxLatInt = maxLatCell;
					} else {
						// maxLat <= maxLatCell;
						maxLatInt = maxLat;
						cell.addCornerPoint(1);
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit);
					}
					cell.addRatioV((maxLatInt - minLatInt) / latBucketLenUnit);
				} else {
					// minLatCell <= minLat
					minLatInt = minLat;
					if (maxLatCell < maxLat) {
						maxLatInt = maxLatCell;
						cell.addCornerPoint(1);
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit);
					} else {
						// maxLat <= maxLatCell;
						maxLatInt = maxLat;
						cell.addCornerPoint(2);
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit * 2);
					}
					cell.addRatioV((maxLatInt - minLatInt) / latBucketLenUnit);
				}

			} else {

				// maxLon <= maxLonCell
				maxLonInt = maxLon;
				if (minLat < minLatCell) {
					minLatInt = minLatCell;
					if (maxLatCell < maxLat) {
						maxLatInt = maxLatCell;
					} else {
						// maxLat <= maxLatCell;
						maxLatInt = maxLat;
						cell.addCornerPoint(2);
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit);
					}
					cell.addRatioV((maxLatInt - minLatInt) / latBucketLenUnit * 2);
				} else {
					// minLatCell <= minLat
					minLatInt = minLat;
					if (maxLatCell < maxLat) {
						maxLatInt = maxLatCell;
						cell.addCornerPoint(2);
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit);
					} else {
						// maxLat <= maxLatCell;
						maxLatInt = maxLat;
						cell.addCornerPoint(4);
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit * 2);
					}
					cell.addRatioV((maxLatInt - minLatInt) / latBucketLenUnit * 2);
				}

			}

		}

		double area = (maxLonInt - minLonInt) * (maxLatInt - minLatInt);

		cell.addRatioArea(area / lonBucketLenUnit / latBucketLenUnit);

		return cell;
	}
	
	private GHCell fillCellNonUniform(double minLon, double minLat, double maxLon, double maxLat, double minLonCell,
			double minLatCell, double maxLonCell, double maxLatCell) {
		GHCell cell = new GHCell(minLonCell, minLatCell, maxLonCell, maxLatCell);

		double lonBucketLenUnit = maxLonCell - minLonCell;
		double latBucketLenUnit = maxLatCell - minLatCell;
				
		double minLonInt = 0.0;
		double maxLonInt = 0.0;
		double minLatInt = 0.0;
		double maxLatInt = 0.0;

		if (minLon < minLonCell) {
			minLonInt = minLonCell;

			if (maxLonCell < maxLon) {
				maxLonInt = maxLonCell;
				if (minLat < minLatCell) {
					minLatInt = minLatCell;

					if (maxLatCell < maxLat) {
						// cell in object
						maxLatInt = maxLatCell;
					} else {
						// maxLat <= maxLatCell;
						maxLatInt = maxLat;
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit);
					}

				} else {
					// minLatCell <= minLat
					minLatInt = minLat;

					if (maxLatCell < maxLat) {
						maxLatInt = maxLatCell;
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit);
					} else {
						// maxLat <= maxLatCell;
						maxLatInt = maxLat;
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit * 2);
					}
				}

			} else {
				// !!! might intersection vertices
				// maxLon <= maxLonCell
				maxLonInt = maxLon;
				if (minLat < minLatCell) {
					minLatInt = minLatCell;
					if (maxLatCell < maxLat) {
						// No intersection vertices
						maxLatInt = maxLatCell;
					} else {
						// maxLat <= maxLatCell;
						maxLatInt = maxLat;
						cell.addCornerPoint(1);
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit);
					}
					cell.addRatioV((maxLatInt - minLatInt) / latBucketLenUnit);
				} else {
					// minLatCell <= minLat
					minLatInt = minLat;
					if (maxLatCell < maxLat) {
						maxLatInt = maxLatCell;
						cell.addCornerPoint(1);
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit);
					} else {
						// maxLat <= maxLatCell;
						maxLatInt = maxLat;
						cell.addCornerPoint(2);
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit * 2);
					}
					cell.addRatioV((maxLatInt - minLatInt) / latBucketLenUnit);
				}

			}

		} else {
			// minLonCell <= minLon
			// no HL
			minLonInt = minLon;
			if (maxLonCell < maxLon) {
				maxLonInt = maxLonCell;
				if (minLat < minLatCell) {
					minLatInt = minLatCell;
					if (maxLatCell < maxLat) {
						// No intersection vertices
						maxLatInt = maxLatCell;
					} else {
						// maxLat <= maxLatCell;
						maxLatInt = maxLat;
						cell.addCornerPoint(1);
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit);
					}
					cell.addRatioV((maxLatInt - minLatInt) / latBucketLenUnit);
				} else {
					// minLatCell <= minLat
					minLatInt = minLat;
					if (maxLatCell < maxLat) {
						maxLatInt = maxLatCell;
						cell.addCornerPoint(1);
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit);
					} else {
						// maxLat <= maxLatCell;
						maxLatInt = maxLat;
						cell.addCornerPoint(2);
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit * 2);
					}
					cell.addRatioV((maxLatInt - minLatInt) / latBucketLenUnit);
				}

			} else {

				// maxLon <= maxLonCell
				maxLonInt = maxLon;
				if (minLat < minLatCell) {
					minLatInt = minLatCell;
					if (maxLatCell < maxLat) {
						maxLatInt = maxLatCell;
					} else {
						// maxLat <= maxLatCell;
						maxLatInt = maxLat;
						cell.addCornerPoint(2);
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit);
					}
					cell.addRatioV((maxLatInt - minLatInt) / latBucketLenUnit * 2);
				} else {
					// minLatCell <= minLat
					minLatInt = minLat;
					if (maxLatCell < maxLat) {
						maxLatInt = maxLatCell;
						cell.addCornerPoint(2);
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit);
					} else {
						// maxLat <= maxLatCell;
						maxLatInt = maxLat;
						cell.addCornerPoint(4);
						cell.addRatioH((maxLonInt - minLonInt) / lonBucketLenUnit * 2);
					}
					cell.addRatioV((maxLatInt - minLatInt) / latBucketLenUnit * 2);
				}

			}

		}

		double area = (maxLonInt - minLonInt) * (maxLatInt - minLatInt);

		cell.addRatioArea(area / lonBucketLenUnit / latBucketLenUnit);

		return cell;
	}

	public int[] getBId(double latitude, double longitude) {

		int[] point = new int[2];

		int xP = -1;
		int yP = -1;

		if (latitude <= minLat) {
			xP = 0;
		} else if (latitude >= maxLat) {
			xP = numLatBucket - 1;
		} else {
			xP = (int) ((latitude - minLat) / latBucketLenUnit);
		}
		if (longitude <= minLon) {
			yP = 0;
		} else if (longitude >= maxLon) {
			yP = numLonBucket - 1;
		} else {
			yP = (int) ((longitude - minLon) / lonBucketLenUnit);
		}

		point[0] = xP;
		point[1] = yP;

		return point;
	}
	
	public double computeStandardDeviation() {

		if (totalN == 0) {
			for (int i = 0; i < count.length; i++) {
				for (int j = 0; j < count[0].length; j++) {
					totalN += count[i][j];
				}

			}
		}

		double mean = totalN / count.length / count[0].length;
		standardDeviation = 0;

		for (int i = 0; i < count.length; i++) {
			for (int j = 0; j < count[0].length; j++) {
				standardDeviation += Math.pow(count[i][j] - mean, 2);
			}

		}

		standardDeviation = standardDeviation / count.length / count[0].length;

		standardDeviation = Math.sqrt(standardDeviation);

		return standardDeviation;
	}
	
	public double getLonUnit() {
		return lonBucketLenUnit;
	}

	public double getLatUnit() {
		return latBucketLenUnit;
	}
	
	public double[][] getCount() {
		return count;
	}

	public double[][] getArea() {
		return mSumRatioArea;
	}
	
	public double[][] getRatioH() {
		return mSumRatioH;
	}
	
	public double[][] getRatioV() {
		return mSumRatioV;
	}
	
	public double[] getLonBoundary() {
		return lonBoundary;
	}

	public double[] getLatBoundary() {
		return latBoundary;
	}
	
	public double[][] getData() {
		return count;
	}
	
	public double[][] getTargetAggTotal() {
		return targetAggTotal;
	}
	
	public double getMinLon() {
		return this.minLon;
	}

	public double getMaxLon() {
		return this.maxLon;
	}

	public double getMinLat() {
		return this.minLat;
	}

	public double getMaxLat() {
		return this.maxLat;
	}
	
	public GeometricHistogramOpt loadHistByDataArray(GeometricHistogramOpt hist, double[][] dataArray) {

//		boolean testLoad = false;
//		if (dataArray[0].length == 2) {
//			testLoad = true;
//		}
		for (int i = 0; i < dataArray.length; i++) {
//			hist.addRecord(dataArray[i][0], dataArray[i][1]);
//			if (testLoad) {
//				hist.addRecord(dataArray[i][0], dataArray[i][1],
//						dataArray[i][0], dataArray[i][1]);
//			} else {
				hist.addRecord(dataArray[i][0], dataArray[i][1],
						dataArray[i][2], dataArray[i][3]);
//			}
		}

		return hist;

	}
	
	public GeometricHistogramOpt loadHistByDataArray(GeometricHistogramOpt hist, List<double[]> allDataArray) {

//		System.out.println(allDataArray.size());
		for (int i = 0; i < allDataArray.size(); i++) {
			hist.addRecord(allDataArray.get(i)[0], allDataArray.get(i)[1],
					allDataArray.get(i)[2], allDataArray.get(i)[3]);
//			hist.addRecordNonUnitform(allDataArray.get(i)[0], allDataArray.get(i)[1],
//					allDataArray.get(i)[2], allDataArray.get(i)[3]);
		}

		return hist;
	}
	
	public GeometricHistogramOpt loadHistByDataFile(GeometricHistogramOpt hist, List<double[]> allDataArray) {

//		System.out.println(allDataArray.size());
		for (int i = 0; i < allDataArray.size(); i++) {
//			hist.addRecord(allDataArray.get(i)[0], allDataArray.get(i)[1],
//					allDataArray.get(i)[2], allDataArray.get(i)[3]);
			hist.addRecordNonUnitform(allDataArray.get(i)[0], allDataArray.get(i)[1],
					allDataArray.get(i)[2], allDataArray.get(i)[3]);
		}

		return hist;
	}
	
	public int selectivityEstimation(GeometricHistogramOpt other) {
		double intersectionPoints = 0;
		
		double[][] srcCount = other.getCount();
		double[][] srcRatioArea = other.getArea();
		double[][] srcRatioH = other.getRatioH();
		double[][] srcRatioV = other.getRatioV();
		
		for (int i = 0; i < numLonBucket; i++) {
			for (int j = 0; j < numLatBucket; j++) {
				intersectionPoints += count[i][j] * srcRatioArea[i][j] +
						mSumRatioArea[i][j] * srcCount[i][j] +
						mSumRatioH[i][j] * srcRatioV[i][j] +
						mSumRatioV[i][j] * srcRatioH[i][j];
			}
		}
		
//		System.out.println("intersectionPoints = " + intersectionPoints + 
//				", " + intersectionPoints / 4);
//		System.out.println("num of cells = " + numLatBucket * numLonBucket);
		
		return (int) Math.ceil(intersectionPoints / 4);
	}
	
	public void aggregateHistogramSep(GeometricHistogramOpt src) {
		this.totalN += src.totalN;
		double[][] srcCount = src.getCount();
		double[][] srcRatioArea = src.getArea();
		double[][] srcRatioH = src.getRatioH();
		double[][] srcRatioV = src.getRatioV();
		double[] srcLonBoundary = src.getLonBoundary();
		double[] srcLatBoundary = src.getLatBoundary();
		
//		System.out.println(Arrays.toString(srcRatioArea[0]));
//		System.out.println(Arrays.toString(srcRatioArea[4]));
//		System.out.println(Arrays.toString(srcRatioH[0]));
//		System.out.println(Arrays.toString(srcRatioH[4]));
//		System.out.println(Arrays.toString(srcRatioV[0]));
//		System.out.println(Arrays.toString(srcRatioV[4]));

		int dimensionT = lonBoundary.length;
		int numBucketT = latBoundary.length;

		int dimensionS = srcLonBoundary.length;
		int numBucketSrc = srcLatBoundary.length;

		// optimize the start dimension and start index for src
		int sdStart = UtilsFunctionHistogram.FirstBiggerSearch(minLon, srcLonBoundary);
		int idxSStart = UtilsFunctionHistogram.FirstBiggerSearch(minLat, srcLatBoundary);
		sdStart = Math.max(0, sdStart);
		idxSStart = Math.max(0, idxSStart);
//			System.out.println("sdStart = " + sdStart + ", idxSStart = " + idxSStart);

		int tdStart = UtilsFunctionHistogram.FirstBiggerSearch(src.minLon, lonBoundary);
		int idxTStart = UtilsFunctionHistogram.FirstBiggerSearch(src.minLat, latBoundary);
		tdStart = Math.max(0, tdStart);
		idxTStart = Math.max(0, idxTStart);

		// start from target lon-dimension = 0
		double tMinLon = lonBoundary[tdStart];
		int md = tdStart + 1;
		// start from src lon-dimension = 0
		double sMinLon = srcLonBoundary[sdStart];
		int sd = sdStart + 1;

		while (md < dimensionT && sd < dimensionS) {
			// first while loop-lon intersection
			double tMaxLon = lonBoundary[md];
			double sMaxLon = srcLonBoundary[sd];

			if (UtilsFunction.isOverlapInterval(tMinLon, tMaxLon, sMinLon, sMaxLon)) {
				// for overlap computation
				double overMinLon = Math.max(tMinLon, sMinLon);
				double overMaxLon = Math.min(tMaxLon, sMaxLon);
				double mWo = overMaxLon - overMinLon;
				if (mWo > 1.1102230246251565E-10) {

					// lat initialize
					int idxT = idxTStart + 1;
					int idxS = idxSStart + 1;
					double tMinLat = latBoundary[idxTStart];
//						double tMaxLat = latBoundary[idxT];

					double sMinLat = srcLatBoundary[idxSStart];
//						double sMaxLat = srcLatBoundary[idxS];

					while (idxT < numBucketT && idxS < numBucketSrc) {
						double tMaxLat = latBoundary[idxT];
						double sMaxLat = srcLatBoundary[idxS];
						if (UtilsFunction.isOverlapInterval(tMinLat, tMaxLat, sMinLat, sMaxLat)) {
							double overMinLat = Math.max(tMinLat, sMinLat);
							double overMaxLat = Math.min(tMaxLat, sMaxLat);

							double mHo = overMaxLat - overMinLat;
							if (mHo > 1.1102230246251565E-10) {
								// overlap
								// compute values
								double tmp = mWo * mHo;
								
								double comAreaSource = tmp / (sMaxLon - sMinLon) / (sMaxLat - sMinLat);
								count[md - 1][idxT - 1] += srcCount[sd - 1][idxS - 1] * comAreaSource;
								
//								targetAggTotal[md - 1][idxT - 1] += srcCount[sd - 1][idxS - 1];
								
								// compute values

//								mComAreaSource = tmp / sLonUnit / sLatUnit;
//								mComAreaTarget = tmp / tLonUnit / tLatUnit;
//								mComHTarget = tmp / tLonUnit / sLatUnit;
//								mComVTarget = tmp / tLatUnit / sLonUnit;
								
//								if ((tMaxLon - tMinLon == 0) || (tMaxLat - tMinLat == 0)) {
//									System.out.println("tMinLon = " + tMinLon + ", tMaxLon = " + tMaxLon);
//									System.out.println("tMinLat = " + tMinLat + ", tMaxLat = " + tMaxLat);
//									System.out.println();
//								}
								
								double comAreaTarget = tmp / (tMaxLon - tMinLon) / (tMaxLat - tMinLat);
								// aggregation += mComAreaTarget * scr
								double o = srcRatioArea[sd - 1][idxS - 1] * comAreaTarget;
								mSumRatioArea[md - 1][idxT - 1] += o;
								
								// aggregation += mComHTarget * scr
								double comHTarget = tmp / (tMaxLon - tMinLon) / (sMaxLat - sMinLat);
								double h = srcRatioH[sd - 1][idxS - 1] * comHTarget;
								mSumRatioH[md - 1][idxT - 1] += h;
								
								// aggregation += mComVTarget * scr
								double comVTarget = tmp / (tMaxLat - tMinLat) / (sMaxLon - sMinLon) ;
								double v = srcRatioV[sd - 1][idxS - 1] * comVTarget;
								mSumRatioV[md - 1][idxT - 1] += v;
							}
						}

						// update lat idx
						if (sMaxLat < tMaxLat) {
							sMinLat = sMaxLat;
							idxS++;
						} else {
							tMinLat = tMaxLat;
							idxT++;
						}
					}
				}
			}
			// update lon idx
			if (sMaxLon < tMaxLon) {
				sMinLon = sMaxLon;
				sd++;
			} else {
				tMinLon = tMaxLon;
				md++;
			}

		}
		
//		System.out.println(Arrays.toString(mSumRatioArea[0]));
//		System.out.println(Arrays.toString(mSumRatioArea[4]));
//		System.out.println(Arrays.toString(mSumRatioH[0]));
//		System.out.println(Arrays.toString(mSumRatioH[4]));
//		System.out.println(Arrays.toString(mSumRatioV[0]));
//		System.out.println(Arrays.toString(mSumRatioV[4]));
//		System.out.println();
	}

}
