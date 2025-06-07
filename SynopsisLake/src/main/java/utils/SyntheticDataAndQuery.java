package utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

public class SyntheticDataAndQuery {

	public static void EnlargeDataSpace(String fileFolder, String inputName, String outputName, int enlargeMin,
			int enlargeMax) {
		String inputPath = fileFolder + inputName;
		String outputPath = fileFolder + outputName;
		double enlargeRange = enlargeMax - enlargeMin;
		try {
			FileWriter dataWriter = new FileWriter(outputPath);
			BufferedReader dataReader = new BufferedReader(new FileReader(inputPath));
			String dataLine = dataReader.readLine();
			while (dataLine != null) {
				String[] tmp = dataLine.split(",");
				double x = Double.parseDouble(tmp[0]);
				double y = Double.parseDouble(tmp[1]);
				double updateX = enlargeMin + enlargeRange * x;
				double updateY = enlargeMin + enlargeRange * y;
				String updateData = updateX + "," + updateY + "\n";
				dataWriter.write(updateData);

				dataLine = dataReader.readLine();
			}
			dataReader.close();
			dataWriter.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// querySize: 0 - small query, 1 - medium query, 2 - long query
	// resolution = 128 * 128
	public static void RangeQuery(String fileFolder, String dataPath, String queryName, int numOfRangeQuery,
			int querySize, double minX, double maxX, double minY, double maxY) {
		String filePath = fileFolder + dataPath;
		String queryPath = fileFolder + queryName;
		Random rand = new Random();
		try {
			ArrayList<double[]> dataList = new ArrayList<double[]>();
			BufferedReader dataReader = new BufferedReader(new FileReader(filePath));
			String dataStr = dataReader.readLine();
			while (dataStr != null) {
				String[] tmp = dataStr.split(",");
				if (dataPath.startsWith("wild")) {
					tmp = dataStr.split("\t");
				}
				double min = Double.parseDouble(tmp[0]);
				double max = Double.parseDouble(tmp[1]);
				double[] data = new double[2];
				data[0] = min;
				data[1] = max;
				dataList.add(data);
				dataStr = dataReader.readLine();
			}
			dataReader.close();

			FileWriter writer = new FileWriter(queryPath);
			double cellSizeX = (maxX - minX) / 128;
			double cellSizeY = (maxY - minY) / 128;
			int dataListSize = dataList.size();
			for (int i = 0; i < numOfRangeQuery; i++) {
				double[] data = dataList.get(rand.nextInt(dataListSize));

				double queryXMin = data[0];
				double queryLenX = 0;
				double queryYMin = data[1];
				double queryLenY = 0;
				if (querySize == 0) {
					// small query
					queryLenX = cellSizeX / 10;
					queryLenY = cellSizeY / 10;
					// ratio = 10^-2
				} else if (querySize == 1) {
					// medium query
					queryLenX = cellSizeX / 2;
					queryLenY = cellSizeY / 2;
				} else {
					// long query
					queryLenX = cellSizeX * 1.5;
					queryLenY = cellSizeY * 1.5;
				}

				queryXMin -= queryLenX / 2;
				queryYMin -= queryLenY / 2;

				double queryXMax = 0;
				if ((queryXMin + queryLenX) >= maxX) {
					queryXMax = queryXMin;
					queryXMin = queryXMax - queryLenX;
				} else {
					queryXMax = queryXMin + queryLenX;
				}

				double queryYMax = 0;
				if ((queryYMin + queryLenY) >= maxY) {
					queryYMax = queryYMin;
					queryYMin = queryYMax - queryLenY;
				} else {
					queryYMax = queryYMin + queryLenY;
				}

				String queryStr = queryXMin + "," + queryXMax + "," + queryYMin + "," + queryYMax + "\n";
				writer.write(queryStr);
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void QueryAns(String fileFolder, String fileName, String queryName, String queryAnsName) {
		String filePath = fileFolder + fileName;
		String queryPath = fileFolder + queryName;
		String queryAnsPath = fileFolder + queryAnsName;

		try {
			ArrayList<double[]> dataList = new ArrayList<double[]>();
			BufferedReader dataReader = new BufferedReader(new FileReader(filePath));
			String dataStr = dataReader.readLine();
			while (dataStr != null) {
				String[] tmp = dataStr.split(",");
				if (fileName.startsWith("wild")) {
					tmp = dataStr.split("\t");
				}
				double min = Double.parseDouble(tmp[0]);
				double max = Double.parseDouble(tmp[1]);
				double[] data = new double[2];
				data[0] = min;
				data[1] = max;
				dataList.add(data);
				dataStr = dataReader.readLine();
			}
			dataReader.close();
			BufferedReader queryReader = new BufferedReader(new FileReader(queryPath));
			String queryStr = queryReader.readLine();
			int cnt = 0;
			ArrayList<double[]> queries = new ArrayList<double[]>();
			while (queryStr != null) {
				String[] tmp = queryStr.split(",");
				double xMin = Double.parseDouble(tmp[0]);
				double xMax = Double.parseDouble(tmp[1]);
				double yMin = Double.parseDouble(tmp[2]);
				double yMax = Double.parseDouble(tmp[3]);

				double[] qAns = new double[5];
				qAns[0] = xMin;
				qAns[1] = xMax;
				qAns[2] = yMin;
				qAns[3] = yMax;
				qAns[4] = 0;
				queries.add(qAns);
				queryStr = queryReader.readLine();
			}
			queryReader.close();
			System.out.println("finish queries: " + queries.size() + "," + " data size = " + dataList.size());
			for (int i = 0; i < dataList.size(); i++) {
				double[] data = dataList.get(i);
				for (int j = 0; j < queries.size(); j++) {
					double[] qAns = queries.get(j);
					double xMin = qAns[0];
					double xMax = qAns[1];
					double yMin = qAns[2];
					double yMax = qAns[3];
					if (UtilsFunction.isOverlapPointCell(xMin, yMin, xMax, yMax, data[0], data[1])) {
						queries.get(j)[4]++;
					}
				}
				if ((i % 500000) == 0) {
					System.out.println(i);
				}
			}
			FileWriter writer = new FileWriter(queryAnsPath);
			for (int j = 0; j < queries.size(); j++) {
				double[] query = queries.get(j);
				double xMin = query[0];
				double xMax = query[1];
				double yMin = query[2];
				double yMax = query[3];
				double qAns = query[4];
				String queryWithAnsStr = xMin + "," + xMax + "," + yMin + "," + yMax + "," + qAns + "\n";
				writer.write(queryWithAnsStr);
			}
			writer.close();
			System.out.println("0-query = " + cnt);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void GetBoundary(String fileFolder, String inputName) {
		String inputPath = fileFolder + inputName;
		try {
			double rangeXMin = Double.MAX_VALUE;
			double rangeXMax = -Double.MIN_VALUE;
			double rangeYMin = Double.MAX_VALUE;
			double rangeYMax = -Double.MIN_VALUE;
			BufferedReader dataReader = new BufferedReader(new FileReader(inputPath));
			String dataLine = dataReader.readLine();
			while (dataLine != null) {
				String[] tmp = dataLine.split("\t");
				double x = Double.parseDouble(tmp[0]);
				double y = Double.parseDouble(tmp[1]);
				rangeXMin = Math.min(rangeXMin, x);
				rangeXMax = Math.max(rangeXMax, x);
				rangeYMin = Math.min(rangeYMin, y);
				rangeYMax = Math.max(rangeYMax, y);
				dataLine = dataReader.readLine();
			}
			dataReader.close();
			System.out.println(rangeXMin + "-" + rangeXMax);
			System.out.println(rangeYMin + "-" + rangeYMax);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String fileFolder = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/"
				+ "Spatial/sketches/icde2025/exp/combine/";
		int enlargeMin = 15;
		int enlargeMax = 45;

//		String outputName = "wildfiredb_shuffle.csv";
//		String queryName = "wild_small_query.txt";
		String inputName = "bit65.csv";
		String outputName = "bit65New.csv";
		String queryName = "syn80_long_query.txt";
		int numOfRangeQuery = 100000;
		int querySize = 2;
//		double minX = -124.66016074688953;
//		double maxX = -4.9E-324;
//		double minY = 24.56637998036461;
//		double maxY = 48.99404003120753;

		double minX = 0;
		double maxX = 1;
		double minY = 0;
		double maxY = 1;

		String queryAnsName = "wild_long_query_ans.txt";

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-numOfRangeQuery")) {
				numOfRangeQuery = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-fileFolder")) {
				fileFolder = args[++i];
			} else if (args[i].equals("-outputName")) {
				outputName = args[++i];
			} else if (args[i].equals("-queryName")) {
				queryName = args[++i];
			} else if (args[i].equals("-querySize")) {
				querySize = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-minX")) {
				minX = Double.parseDouble(args[++i]);
			} else if (args[i].equals("-maxX")) {
				maxX = Double.parseDouble(args[++i]);
			} else if (args[i].equals("-minY")) {
				minY = Double.parseDouble(args[++i]);
			} else if (args[i].equals("-maxY")) {
				maxY = Double.parseDouble(args[++i]);
			} else if (args[i].equals("-queryAnsName")) {
				queryAnsName = args[++i];
			}
		}

		EnlargeDataSpace(fileFolder, inputName, outputName, enlargeMin, enlargeMax);
//		RangeQuery(fileFolder, outputName, queryName, numOfRangeQuery, querySize, minX, maxX, minY, maxY);
//		QueryAns(fileFolder, outputName, queryName, queryAnsName);
//		GetBoundary(fileFolder, inputName);
	}

}
