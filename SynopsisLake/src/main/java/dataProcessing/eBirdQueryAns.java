package dataProcessing;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import utils.UtilsFunction;

public class eBirdQueryAns {

	String folder = "";
	String filePath = "";
	String queryName = "";
	String outputName = "";

	public eBirdQueryAns(String folder, String fileName, String queryName, String outputName,
			int queryIdx) {

		this.folder = folder;
		this.filePath = folder + fileName;
		this.queryName = folder + "rangeQueryAns/" + queryName;
		this.outputName = folder + "rangeQueryAns1D/" + outputName;

		// step 1: load all query
		ArrayList<double[]> queries = readQueries(queryIdx);

		// step 2: query ans
		// for each data point, go over queries
		// if in query range, ans+=1
		queries = readData(queries, queryIdx);

		// step 3: write ans
		writeAns(queries, queryIdx);
	}

	public void writeAns(ArrayList<double[]> queries, int queryIdx) {
		FileWriter myWriter;
		String ansPath = outputName;
		try {
			myWriter = new FileWriter(ansPath, false);
			System.out.println("query path = " + ansPath);
//			int cnt = 0;
			for (int qId = 0; qId < queries.size(); qId++) {
				if (queryIdx == 2) {
					double minLon = queries.get(qId)[0];
					double maxLon = queries.get(qId)[1];
					double countAns = queries.get(qId)[2];
					double ans = queries.get(qId)[3];
					String query = minLon + "," + maxLon + "," + countAns + ","+ ans + "\n";
					myWriter.write(query);
				} else {
					double minLon = queries.get(qId)[0];
					double maxLon = queries.get(qId)[1];
					double countAns = queries.get(qId)[2];
					String query = minLon + "," + maxLon + "," + countAns + "\n";
					myWriter.write(query);
				}
			}

//			System.out.println("cnt = " + cnt);

			myWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public ArrayList<double[]> readData(ArrayList<double[]> queries,
			int querIdx) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			String line = reader.readLine();

			while (line != null) {
				String[] tmp = null;
				if (querIdx == 2) {
					tmp = line.split(",");
				} else {
					tmp = line.split("\t");
				}
				
				double x = Double.parseDouble(tmp[0].trim());
//				double y = Double.parseDouble(tmp[1].trim());
				for (int qId = 0; qId < queries.size(); qId++) {
					double minLon = queries.get(qId)[0];
//					double minLat = queries.get(qId)[1];
					double maxLon = queries.get(qId)[1];
					if (UtilsFunction.isOverlapPointInterval(minLon, maxLon, x)){
						queries.get(qId)[2]++;
					}
					if (querIdx == 2) {
						queries.get(qId)[3]+= Double.parseDouble(tmp[3].trim());
					} 
				}
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
		return queries;
	}

	public ArrayList<double[]> readQueries(int queryIdx) {
		ArrayList<double[]> queries = new ArrayList<double[]>();

		String queryPath = queryName;

		FileReader fr;
		try {
			fr = new FileReader(queryPath);
			BufferedReader br = new BufferedReader(fr);
			String line = br.readLine();
			while (line != null) {
				String[] tem = line.split(",");
				double[] query = null;
				if (queryIdx == 2) {
					query = new double[4];
					query[0] = Double.parseDouble(tem[0]);
					query[1] = Double.parseDouble(tem[1]);
				} else {
					query = new double[3];
					query[0] = Double.parseDouble(tem[0]);
					query[1] = Double.parseDouble(tem[1]);
				}
				// minlon, minlat, maxlon, maxlat, ans
				query[0] = Double.parseDouble(tem[0]);
				query[1] = Double.parseDouble(tem[2]);
				queries.add(query);
				line = br.readLine();
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("num query = " + queries.size());
		return queries;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String folder = "";
		String queryName = "syn-3-1024RQ_sample.txt";
//		String dsName = "wildfire_shuffle/";
		String dsName = "synthetic/";
		String outputName = "syn-3-1024RQ_sample_ans.txt";
		int queryIdx = 2;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-folder")) {
				folder = args[++i];
			} else if (args[i].equals("-queryName")) {
				queryName = args[++i];
			} else if (args[i].equals("-dsName")) {
				dsName = args[++i];
			} else if (args[i].equals("-outputName")) {
				outputName = args[++i];
			} else if (args[i].equals("-queryIdx")) {
				queryIdx = Integer.parseInt(args[++i]);
			}
		}

		eBirdQueryAns run = new eBirdQueryAns(folder, dsName, queryName, outputName, queryIdx);

	}

}
