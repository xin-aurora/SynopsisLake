package histogram;

import dataStructure.GHCell;

public class GeometricHistogram extends SpatialHistogram {

	// histogram cells
	private GHCell[][] mCells;

	public GeometricHistogram(double minLon, double minLat, double maxLon, double maxLat, int numLonBucket,
			int numLatBucket) {
		super(minLon, minLat, maxLon, maxLat, numLonBucket, numLatBucket);

		mCells = new GHCell[mNumLatBucket][mNumLonBucket];

		for (int i = 0; i < mNumLatBucket; i++) {
			for (int j = 0; j < mNumLonBucket; j++) {
				double cellMinLon = mMinLon + j * mLonUnitBucket;
				double cellMaxLon = cellMinLon + mLonUnitBucket;
				double cellMinLat = mMinLat + i * mLatUnitBucket;
				double cellMaxLat = cellMinLat + mLatUnitBucket;
				mCells[i][j] = new GHCell(cellMinLon, cellMinLat, cellMaxLon, cellMaxLat);
			}
		}
	}

	public void addRecord(double minLon, double minLat, double maxLon, double maxLat) {

		// --> lon
		// P1 - P2
		// P3 - P4
		int[] p1 = getBId(minLat, minLon);
		int[] p2 = getBId(minLat, maxLon);
		int[] p3 = getBId(maxLat, minLon);

		for (int lat = p1[0]; lat <= p3[0]; lat++) {
			for (int lon = p1[1]; lon <= p2[1]; lon++) {
//				int idx = lon + lat * mNumLonBucket;
//				System.out.println("idx = " + idx);
				double cellMinLon = mMinLon + lon * mLonUnitBucket;
				double cellMaxLon = cellMinLon + mLonUnitBucket;
				double cellMinLat = mMinLat + lat * mLatUnitBucket;
				double cellMaxLat = cellMinLat + mLatUnitBucket;

//				System.out.println("Cell: " + cellMinLon + "-" + cellMaxLon + ", " + cellMinLat + "-" + cellMaxLat);

				GHCell tmpCell = fillCell(minLon, minLat, maxLon, maxLat, cellMinLon, cellMinLat, cellMaxLon,
						cellMaxLat);
				mCells[lat][lon].accumulate(tmpCell);

//				System.out.println(lat + "-" + lon + ": " + mCells[lat][lon]);
//				System.out.println();
			}
		}
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
						cell.addRatioH((maxLonInt - minLonInt) / mLonUnitBucket);
					}

				} else {
					// minLatCell <= minLat
					minLatInt = minLat;

					if (maxLatCell < maxLat) {
						maxLatInt = maxLatCell;
						cell.addRatioH((maxLonInt - minLonInt) / mLonUnitBucket);
					} else {
						// maxLat <= maxLatCell;
						maxLatInt = maxLat;
						cell.addRatioH((maxLonInt - minLonInt) / mLonUnitBucket * 2);
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
						cell.addRatioH((maxLonInt - minLonInt) / mLonUnitBucket);
					}
					cell.addRatioV((maxLatInt - minLatInt) / mLatUnitBucket);
				} else {
					// minLatCell <= minLat
					minLatInt = minLat;
					if (maxLatCell < maxLat) {
						maxLatInt = maxLatCell;
						cell.addCornerPoint(1);
						cell.addRatioH((maxLonInt - minLonInt) / mLonUnitBucket);
					} else {
						// maxLat <= maxLatCell;
						maxLatInt = maxLat;
						cell.addCornerPoint(2);
						cell.addRatioH((maxLonInt - minLonInt) / mLonUnitBucket * 2);
					}
					cell.addRatioV((maxLatInt - minLatInt) / mLatUnitBucket);
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
						cell.addRatioH((maxLonInt - minLonInt) / mLonUnitBucket);
					}
					cell.addRatioV((maxLatInt - minLatInt) / mLatUnitBucket);
				} else {
					// minLatCell <= minLat
					minLatInt = minLat;
					if (maxLatCell < maxLat) {
						maxLatInt = maxLatCell;
						cell.addCornerPoint(1);
						cell.addRatioH((maxLonInt - minLonInt) / mLonUnitBucket);
					} else {
						// maxLat <= maxLatCell;
						maxLatInt = maxLat;
						cell.addCornerPoint(2);
						cell.addRatioH((maxLonInt - minLonInt) / mLonUnitBucket * 2);
					}
					cell.addRatioV((maxLatInt - minLatInt) / mLatUnitBucket);
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
						cell.addRatioH((maxLonInt - minLonInt) / mLonUnitBucket);
					}
					cell.addRatioV((maxLatInt - minLatInt) / mLatUnitBucket * 2);
				} else {
					// minLatCell <= minLat
					minLatInt = minLat;
					if (maxLatCell < maxLat) {
						maxLatInt = maxLatCell;
						cell.addCornerPoint(2);
						cell.addRatioH((maxLonInt - minLonInt) / mLonUnitBucket);
					} else {
						// maxLat <= maxLatCell;
						maxLatInt = maxLat;
						cell.addCornerPoint(4);
						cell.addRatioH((maxLonInt - minLonInt) / mLonUnitBucket * 2);
					}
					cell.addRatioV((maxLatInt - minLatInt) / mLatUnitBucket * 2);
				}

			}

		}

		double area = (maxLonInt - minLonInt) * (maxLatInt - minLatInt);

		cell.addRatioArea(area / mCellArea);

		return cell;
	}

	public GHCell[][] getCells() {
		return mCells;
	}

	public GHCell getCellById(int lat, int lon) {
		return mCells[lat][lon];
	}

	public void fillCellById(int lat, int lon, GHCell cell) {
		mCells[lat][lon].accumulate(cell);
	}

	public void updateCornerPoint(int lat, int lon, int c) {
		mCells[lat][lon].addCornerPoint(c);
	}

	public void updateRatioArea(int lat, int lon, double o) {
		mCells[lat][lon].addRatioArea(o);
	}

	public void updateRatioH(int lat, int lon, double h) {
		mCells[lat][lon].addRatioH(h);
	}

	public void updateRatioV(int lat, int lon, double v) {
		mCells[lat][lon].addRatioV(v);
	}
	
	public int selectivityEstimation(GeometricHistogram other) {
		double intersectionPoints = 0;
		
		for (int i = 0; i < mNumLatBucket; i++) {
			for (int j = 0; j < mNumLonBucket; j++) {
				GHCell otherCell = other.mCells[i][j];
				intersectionPoints += mCells[i][j].getmNumC() * otherCell.getmSumRatioArea() +
						mCells[i][j].getmSumRatioArea() * otherCell.getmNumC() +
						mCells[i][j].getmSumRatioH() * otherCell.getmSumRatioV() +
						mCells[i][j].getmSumRatioV() * otherCell.getmSumRatioH();
			}
		}
		
		System.out.println("intersectionPoints = " + intersectionPoints + 
				", " + intersectionPoints / 4);
		System.out.println("num of cells = " + mNumLatBucket * mNumLonBucket);
		
		return (int) Math.ceil(intersectionPoints / 4);
	}

}
