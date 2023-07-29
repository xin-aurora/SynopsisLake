package histogram;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;

import dataStructure.SSH2DCell;
import dataStructure.WaveletCoeff;
import dataStructure.queue.WaveletThreRankObj;

public class Wavelet2D implements Serializable{
	WaveletCoeff[][] coeffArrays;

//	boolean rowNorm = false;
//	boolean columnNorm = false;
	
	public Wavelet2D(SimpleSpatialHistogramOpt hist) {
		double[][] dataArrays = hist.getData();
		int d1Len = dataArrays.length;
		int d2Len = dataArrays[0].length;
		
		coeffArrays = new WaveletCoeff[d1Len][d2Len];

		computeCoeffArrays(dataArrays);
	}

	public Wavelet2D(double[][] dataArrays, boolean prefixSum) {
		int d1Len = dataArrays.length;
		int d2Len = dataArrays[0].length;

//		System.out.println("d1Len = " + d1Len + ", d2Len = " + d2Len);
		coeffArrays = new WaveletCoeff[d1Len][d2Len];

//		System.out.println("Data array = " + Arrays.toString(data));

		computeCoeffArrays(dataArrays);
//		System.out.println("normalization = " + normalization);
	}

	private void computeCoeffArrays(double[][] dataArrays) {

		// compute row
		for (int row = 0; row < dataArrays.length; row++) {
			computeCoeffByCell(dataArrays[row], row);
			rowNormalization(row);
		}

		// compute column
		for (int column = 0; column < dataArrays[0].length; column++) {
			WaveletCoeff[] columnDataArray = new WaveletCoeff[dataArrays.length];
			for (int row = 0; row < dataArrays.length; row++) {
				columnDataArray[row] = coeffArrays[row][column];
			}
			computeCoeffByWavelet(columnDataArray, column);
			columnNormalization(column);
		}
	}

	private void computeCoeffByCell(double[] dataArray, int rowId) {

		int totalHeight = (int) (Math.log(dataArray.length) / Math.log(2)) - 1;
//		System.out.println("total height = " + totalHeight);

		Queue<WaveletCoeff> queue = new LinkedList<WaveletCoeff>();

		int[] heightArray = new int[2];
		for (int i = 0; i < dataArray.length; i++) {
			queue.add(new WaveletCoeff(dataArray[i], heightArray, i));
		}

		while (!queue.isEmpty()) {
			heightArray = new int[2];
			WaveletCoeff first = queue.poll();
			if (!queue.isEmpty()) {
				WaveletCoeff second = queue.poll();
				// compute difference
				double diff = (first.getVal() - second.getVal()) / 2;
				// pos = 2^(totalHeight - heigh) + key/2
				int pos = computePos(totalHeight, first.getHeight()[0], first.getKey());
				heightArray[0] = totalHeight - first.getHeight()[0];
				coeffArrays[rowId][pos] = new WaveletCoeff(diff, heightArray, -1);
				double avg = (first.getVal() + second.getVal()) / 2;
				heightArray[0] = first.getHeight()[0] + 1;
				queue.add(new WaveletCoeff(avg, heightArray, first.getKey() / 2));

			} else {
				// the last element in queue,
				// the first element in coefficient array
				// it is the root of error tree

				heightArray[0] = totalHeight - first.getHeight()[0] + 1;
				coeffArrays[rowId][0] = new WaveletCoeff(first.getVal(), heightArray, -1);

			}
		}

	}

	private void computeCoeffByWavelet(WaveletCoeff[] dataArray, int columnId) {

		int totalHeight = (int) (Math.log(dataArray.length) / Math.log(2)) - 1;
//		System.out.println("total height = " + totalHeight);

		Queue<WaveletCoeff> queue = new LinkedList<WaveletCoeff>();

		for (int i = 0; i < dataArray.length; i++) {
			queue.add(new WaveletCoeff(dataArray[i].getVal(), dataArray[i].getHeight(), i));
		}

		int[] heightArray = new int[2];
		while (!queue.isEmpty()) {
			heightArray = new int[2];
			WaveletCoeff first = queue.poll();
			if (!queue.isEmpty()) {
				WaveletCoeff second = queue.poll();
				// compute difference
				double diff = (first.getVal() - second.getVal()) / 2;
				// pos = 2^(totalHeight - heigh) + key/2
				int pos = computePos(totalHeight, first.getHeight()[1], first.getKey());
				heightArray[0] = first.getHeight()[0];
				heightArray[1] = totalHeight - first.getHeight()[1];
				coeffArrays[pos][columnId] = new WaveletCoeff(diff, heightArray, -1);
				double avg = (first.getVal() + second.getVal()) / 2;
				heightArray[1] = first.getHeight()[1] + 1;
				queue.add(new WaveletCoeff(avg, heightArray, first.getKey() / 2));

			} else {
				// the last element in queue,
				// the first element in coefficient array
				// it is the root of error tree

				heightArray[0] = first.getHeight()[0];
				heightArray[1] = totalHeight - first.getHeight()[1] + 1;
				coeffArrays[0][columnId] = new WaveletCoeff(first.getVal(), heightArray, -1);

			}
		}

	}

	private int computePos(int totalHeight, int height, int key) {
		return (int) Math.pow(2, totalHeight - height) + key / 2;
	}

	// normalization = sqrt(m / 2^l)
	// m: size of data array
	// l: level of resolution
	public void rowNormalization(int rowId) {
		int m = coeffArrays.length;
		for (int i = 0; i < m; i++) {
			double normScale = Math.sqrt(m / Math.pow(2, coeffArrays[rowId][i].getHeight()[0]));
//					System.out.println("normScale = " + normScale);
			coeffArrays[rowId][i].updateVal(coeffArrays[rowId][i].getVal() * normScale);
		}
	}

	public void columnNormalization(int columnId) {
		int m = coeffArrays[0].length;
		for (int i = 0; i < m; i++) {
			double normScale = Math.sqrt(m / Math.pow(2, coeffArrays[i][columnId].getHeight()[1]));
//					System.out.println("normScale = " + normScale);
			coeffArrays[i][columnId].updateVal(coeffArrays[i][columnId].getVal() * normScale);
		}
	}

	public void rowUnnormalization(int rowId) {
		int m = coeffArrays.length;
		for (int i = 0; i < m; i++) {
			double normScale = Math.sqrt(m / Math.pow(2, coeffArrays[rowId][i].getHeight()[0]));
//			System.out.println("unnormScale = " + normScale);
			coeffArrays[rowId][i].updateVal(coeffArrays[rowId][i].getVal() / normScale);
		}
	}

	public void columnUnnormalization(int columnId) {
		int m = coeffArrays[0].length;
		for (int i = 0; i < m; i++) {
			double normScale = Math.sqrt(m / Math.pow(2, coeffArrays[i][columnId].getHeight()[1]));
//			System.out.println("normScale = " + normScale);
			coeffArrays[i][columnId].updateVal(coeffArrays[i][columnId].getVal() / normScale);
		}
	}

	public WaveletCoeff[][] getCoeffArray() {
		return coeffArrays;
	}
	
	public String[] getCoeffStringArray() {
		String[] coeffStringArrays = new String[coeffArrays.length];
		for (int i = 0; i < coeffArrays.length; i++) {
			String dataStr = Arrays.toString(coeffArrays[i]) + "\n";
			coeffStringArrays[i] = dataStr;
		}
		return coeffStringArrays;
	}

	public void threshold(int boundC) {

//		char[][] symbols = new char[coeffArrays.length][coeffArrays[0].length];
		int boundCFirst = (int) (boundC * 1.5);

		PriorityQueue<WaveletThreRankObj> queue = new PriorityQueue<WaveletThreRankObj>(
				new Comparator<WaveletThreRankObj>() {

					@Override
					public int compare(WaveletThreRankObj o1, WaveletThreRankObj o2) {
						// TODO Auto-generated method stub
						return Double.compare(o1.v, o2.v);
					}
				});

		int cnt = 0;
		for (int row = 0; row < coeffArrays.length; row++) {
			for (int column = 0; column < coeffArrays[0].length; column++) {
				int[] idxs = new int[2];
				idxs[0] = row;
				idxs[1] = column;
				int weight = 0;
				if (row == 0) {
					weight++;
				}
				if (column == 0) {
					weight++;
				}
				WaveletThreRankObj newObj = new WaveletThreRankObj(Math.abs(coeffArrays[row][column].getVal()), weight,
						idxs);
				if (queue.size() < boundCFirst) {
					queue.add(newObj);
//					symbols[row][column] = 'o';
//					System.out.println("cnt = " + cnt + ", queue size = " + queue.size());
				} else {
//					System.out.println("remove");
					// then queue.size() == boundC
					// compare the peek
					cnt++;
					if (newObj.v > queue.peek().v) {
						// remove peek, add new obj
						WaveletThreRankObj rmObj = queue.poll();
						coeffArrays[rmObj.idxs[0]][rmObj.idxs[1]].updateVal(0);
//						symbols[rmObj.idxs[0]][rmObj.idxs[1]] = 'x';
//						System.out.println("remove " + Arrays.toString(rmObj.idxs));
						queue.add(newObj);
//						symbols[row][column] = 'o';
//						System.out.println("cnt = " + cnt + ", queue size = " + queue.size());
					} else {
						// remove newObj
						coeffArrays[idxs[0]][idxs[1]].updateVal(0);
//						symbols[row][column] = 'x';
//						System.out.println("remove " + Arrays.toString(idxs));
					}
				}
			}
		}
//		System.out.println("remove " + cnt + " coefficients, remain " + queue.size() + " coefficients.");

		PriorityQueue<WaveletThreRankObj> queueSecond = new PriorityQueue<WaveletThreRankObj>(
				new Comparator<WaveletThreRankObj>() {

					@Override
					public int compare(WaveletThreRankObj o1, WaveletThreRankObj o2) {
						// TODO Auto-generated method stub
						return Double.compare(o1.weight, o2.weight);
					}
				});

//		cnt = 0;
		while (!queue.isEmpty()) {
//			WaveletThreRankObj preObj = queue.poll();
//			int[] idxs = preObj.idxs;
//			int weight = preObj.weight;
//			WaveletThreRankObj newObj = new WaveletThreRankObj(preObj.v, weight,
//					idxs);
			WaveletThreRankObj newObj = queue.poll();
			int[] idxs = newObj.idxs;
			if (queueSecond.size() < boundC) {
				queueSecond.add(newObj);
//					System.out.println("cnt = " + cnt + ", queue size = " + queue.size());
			} else {
//					System.out.println("remove");
				// then queue.size() == boundC
				// compare the peek
				cnt++;
				if (newObj.weight > queueSecond.peek().weight) {
					// remove peek, add new obj
					WaveletThreRankObj rmObj = queueSecond.poll();
					coeffArrays[rmObj.idxs[0]][rmObj.idxs[1]].updateVal(0);
//						System.out.println("remove " + Arrays.toString(rmObj.idxs));
					queueSecond.add(newObj);
//						System.out.println("cnt = " + cnt + ", queue size = " + queue.size());
				} else {
					// remove newObj
					coeffArrays[idxs[0]][idxs[1]].updateVal(0);
//						System.out.println("remove " + Arrays.toString(idxs));
				}
			}

		}
		System.out.println("remove " + cnt + " coefficients, remain " + queueSecond.size() + " coefficients.");
//		return symbols;
	}

	// reverse column first
	// then reverse row
	public double[][] reverse() {
		double[][] reverseArray = new double[coeffArrays.length][coeffArrays[0].length];

		// reverse column first
		for (int column = 0; column < coeffArrays[0].length; column++) {
			reverseArray[0][column] = coeffArrays[0][column].getVal();
//			System.out.println("column = " + column + ": " + reverseArray[0][column]);
			int start = 1;
			int end = 2;
			while (end <= reverseArray.length) {

				for (int i = start; i < end; i++) {
					reverseArray[i][column] = coeffArrays[i][column].getVal();
//					System.out.println("i = " + i + ": " + reverseArray[i][column]);
//					System.out.println(i);
				}

				double[] reverseCurrentArray = updateDataArray(end - start, false, -1, column);
				for (int i = 0; i < end; i++) {
					reverseArray[i][column] = reverseCurrentArray[i];
					coeffArrays[i][column].updateVal(reverseCurrentArray[i]);
				}

				start = end;
				end = end * 2;
			}
		}

//		System.out.println("Reverse array");
//		for (int i=0; i<coeffArrays.length; i++) {
//			System.out.println(Arrays.toString(reverseArray[i]));
//		}

		// then reverse row
		for (int row = 0; row < coeffArrays.length; row++) {
//			System.out.println("row = " + row + ": " + reverseArray[row][0]);
			int start = 1;
			int end = 2;
			while (end <= reverseArray[0].length) {

				double[] reverseCurrentArray = updateDataArray(end - start, true, row, -1);

				for (int i = 0; i < end; i++) {
					reverseArray[row][i] = reverseCurrentArray[i];
					coeffArrays[row][i].updateVal(reverseCurrentArray[i]);
				}

				start = end;
				end = end * 2;
			}
		}

		return reverseArray;
	}

	public double[] updateDataArray(int total, boolean isRow, int rowId, int columnId) {
//		System.out.println(Arrays.toString(reverseArray));
		double[] reverse = null;
		if (isRow) {
			reverse = new double[coeffArrays[0].length];
		} else {
			reverse = new double[coeffArrays.length];
		}
		int idx = 0;
		for (int i = 0; i < total; i++) {
			if (isRow) {
				double first = coeffArrays[rowId][i].getVal() * 2;
				double second = coeffArrays[rowId][i + total].getVal() * 2;
//				System.out.println(first + ", " + second);
				reverse[idx] = (first + second) / 2;
//				System.out.println("f+s = " + reverse[idx]);
				idx++;
				reverse[idx] = (first - second) / 2;
//				System.out.println("f-s = " + reverse[idx]);
				idx++;
			} else {
				double first = coeffArrays[i][columnId].getVal() * 2;
				double second = coeffArrays[i + total][columnId].getVal() * 2;
//				System.out.println("i = " + i + ": "+ first + ", total = " + total + ": " + second);
				reverse[idx] = (first + second) / 2;
//				System.out.println("f+s = " + reverse[idx]);
				idx++;
				reverse[idx] = (first - second) / 2;
//				System.out.println("f-s = " + reverse[idx]);
				idx++;
			}
		}

		return reverse;
	}

	public SSH2DCell[][] reverse(SSH2DCell[][] cells) {

		for (int column = 0; column < coeffArrays[0].length; column++) {
			columnUnnormalization(column);
		}

		// reverse column first
		for (int column = 0; column < coeffArrays[0].length; column++) {
			int start = 1;
			int end = 2;
			while (end <= cells.length) {
				double[] reverseCurrentArray = updateDataArray(end - start, false, -1, column);
				for (int i = 0; i < end; i++) {
					cells[i][column].fillVal(reverseCurrentArray[i]);
					coeffArrays[i][column].updateVal(reverseCurrentArray[i]);
				}

				start = end;
				end = end * 2;
			}
		}

		for (int row = 0; row < coeffArrays.length; row++) {
			rowUnnormalization(row);
		}
		// then reverse row
		for (int row = 0; row < coeffArrays.length; row++) {
			int start = 1;
			int end = 2;
			while (end <= cells[0].length) {

				double[] reverseCurrentArray = updateDataArray(end - start, true, row, -1);

				for (int i = 0; i < end; i++) {
					cells[row][i].fillVal(reverseCurrentArray[i]);
					coeffArrays[row][i].updateVal(reverseCurrentArray[i]);
				}

				start = end;
				end = end * 2;
			}
		}

		return cells;
	}
}
