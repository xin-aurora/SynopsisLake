package dataProcessing;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import histogram.SimpleSpatialHistogramOpt;

public class BuildRangeQuery {

	String folder = "";
	int numFile = 0;
	String dsName = "";
	String regex = "\t";
	String outputName = "";
	int numQuery = 0;
	double scale = 0;

	double rangeMinLon = 0;
	double rangeMaxLon = 0;
	double rangeMinLat = 0;
	double rangeMaxLat = 0;

	int top = 0;

	public BuildRangeQuery(String folder, int numFile, String dsName, String outputName, int numQuery, int base,
			double scale, double rangeMinLon, double rangeMaxLon, double rangeMinLat, double rangeMaxLat) {

		this.folder = folder;
		this.numFile = numFile;
		this.dsName = dsName;
		if (dsName.startsWith("syn")) {
			regex = ",";
		}
//		else {
//			regex = "\t";
//		}
		this.outputName = outputName;
		this.numQuery = numQuery;
		if (scale > -4) {
			top = 20;
		} else {
			top = 2;
		}
		this.scale = Math.pow(base, scale);
		this.rangeMinLon = rangeMinLon;
		this.rangeMinLat = rangeMinLat;
		this.rangeMaxLon = rangeMaxLon;
		this.rangeMaxLat = rangeMaxLat;

		// step 1: read all data
		ArrayList<double[]> allData = readData();

//		// step2: pick range query from hist
//		buildHist(allData);

//		// pick points method
		buildCenterQueries(allData);
		
		// uniform random
//		uniformQueries(allData);
	}

	public void uniformQueries(ArrayList<double[]> allData) {
		// query length
		double qLonLen = (rangeMaxLon - rangeMinLon) / 256 * scale / 2;
		double qLatLen = (rangeMaxLat - rangeMinLat) / 256 * scale / 2;
		
		// random pick points as centers
				Random random = new Random();

				String queryPath = folder + outputName;
				try {
					FileWriter myWriter = new FileWriter(queryPath, false);
					int cnt = 0;
					while (cnt < numQuery) {
						
						double lon = rangeMinLon + (rangeMaxLon - rangeMinLon) * random.nextDouble();
						double lat = rangeMinLat + (rangeMaxLat - rangeMinLat) * random.nextDouble();
						double qMinLon = lon - qLonLen;
						double qMinLat = lat - qLatLen;
						double qMaxLon = lon + qLonLen;
						double qMaxLat = lat + qLatLen;
						String query = qMinLon + "," + qMinLat + "," + qMaxLon + "," + qMaxLat + "\n";
						myWriter.write(query);
						cnt++;
					}

					myWriter.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

	}

	public void buildCenterQueries(ArrayList<double[]> allData) {

		int numPoints = allData.size();

		// query length
		double qLonLen = (rangeMaxLon - rangeMinLon) / 256 * scale / 2;
		double qLatLen = (rangeMaxLat - rangeMinLat) / 256 * scale / 2;

		// random pick points as centers
		Random random = new Random();

		String queryPath = folder + outputName;
		try {
			FileWriter myWriter = new FileWriter(queryPath, false);
			int cnt = 0;
			while (cnt < numQuery) {
				int pId = random.nextInt(numPoints);
				double lon = allData.get(pId)[0];
				double lat = allData.get(pId)[1];
				double qMinLon = lon - qLonLen;
				double qMinLat = lat - qLatLen;
				double qMaxLon = lon + qLonLen;
				double qMaxLat = lat + qLatLen;
				String query = qMinLon + "," + qMinLat + "," + qMaxLon + "," + qMaxLat + "\n";
				myWriter.write(query);
				cnt++;
			}

			myWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void buildHist(ArrayList<double[]> allData) {
		SimpleSpatialHistogramOpt hist = new SimpleSpatialHistogramOpt(rangeMinLon, rangeMinLat, rangeMaxLon,
				rangeMaxLat, 256, 256);
		System.out.println(allData.size());
		for (int dId = 0; dId < allData.size(); dId++) {
			hist.addRecord(allData.get(dId)[0], allData.get(dId)[1]);
		}
		FileWriter myWriter;
		String queryPath = folder + outputName;

		try {
			myWriter = new FileWriter(queryPath, false);
			Random random = new Random();
			double[][] data = hist.getData();
			int cnt = 0;
			// pick top-20
			PriorityQueue<RangeQueryHelper> queue = new PriorityQueue<RangeQueryHelper>(
					new Comparator<RangeQueryHelper>() {

						@Override
						public int compare(RangeQueryHelper o1, RangeQueryHelper o2) {
							// TODO Auto-generated method stub
							return Double.compare(o2.val, o1.val);
						}
					});

			for (int i = 0; i < data.length; i++) {
				for (int j = 0; j < data[0].length; j++) {
					if (data[i][j] > 0) {
						cnt++;
						int[] pos = new int[2];
						pos[0] = i;
						pos[1] = j;
						RangeQueryHelper candidate = new RangeQueryHelper(pos, data[i][j]);
						queue.add(candidate);
					}
				}
			}
			cnt = 0;
			System.out.println("Top candidates");
			RangeQueryHelper[] candidates = new RangeQueryHelper[top];
			while (cnt < top) {
				RangeQueryHelper candidate = queue.poll();
				candidates[cnt] = candidate;
				System.out.println(candidate.val);
				cnt++;
			}
			System.out.println("non-zero cell = " + cnt);
			double[] lonBoundary = hist.getLonBoundary();
			double[] latBoundary = hist.getLatBoundary();
			double qLonLen = hist.getLonUnit() * scale;
			double qLatLen = hist.getLatUnit() * scale;
			cnt = 0;
			while (cnt < numQuery) {
				int candIdx = random.nextInt(top);
				RangeQueryHelper candidate = candidates[candIdx];
				double minLon = lonBoundary[candidate.pos[0]];
				double minLat = latBoundary[candidate.pos[1]];
				double maxLon = lonBoundary[candidate.pos[0] + 1];
				double maxLat = latBoundary[candidate.pos[1] + 1];

				double qMinLon = minLon + (maxLon - minLon) * random.nextDouble();
				double qMinLat = minLat + (maxLat - minLat) * random.nextDouble();
				double qMaxLon = qMinLon + qLonLen;
				double qMaxLat = qMinLat + qLatLen;
				String query = qMinLon + "," + qMinLat + "," + qMaxLon + "," + qMaxLat + "\n";
				myWriter.write(query);

				cnt++;
				if (cnt % 5000 == 0) {
					System.out.println(cnt);
				}

			}
			myWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public ArrayList<double[]> readData() {

		ArrayList<double[]> allData = new ArrayList<double[]>();
		for (int fileId = 0; fileId < numFile; fileId++) {
			String fileP = folder + dsName + "file-" + fileId + ".csv";
			System.out.println(fileP);
			Path filePath = new File(fileP).toPath();
			List<String> allLines;
			try {
				allLines = Files.readAllLines(filePath);
				int totalLine = allLines.size();
				String[] tailInfo = allLines.get(totalLine - 1).split(";");
				String[] synopsisInfo = tailInfo[1].split(",");
				int synopsisStart = Integer.parseInt(synopsisInfo[0]);
				int dataEnd = synopsisStart - 1;
				for (String data : allLines.subList(0, dataEnd)) {
					String[] tmp = data.split(regex);
					double[] dataArray = new double[2];
					dataArray[0] = Double.parseDouble(tmp[0]);
					dataArray[1] = Double.parseDouble(tmp[1]);
					allData.add(dataArray);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return allData;
	}

	public static void main(String[] args) {
		String folder = "";
		int numFile = 0;
		String dsName = "";
		String outputName = "";
		int numQuery = 0;
		int base = 0;
		double scale = 0;

		double rangeMinLon = 0;
		double rangeMaxLon = 0;
		double rangeMinLat = 0;
		double rangeMaxLat = 0;

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-numFile")) {
				numFile = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-folder")) {
				folder = args[++i];
			} else if (args[i].equals("-dsName")) {
				dsName = args[++i];
			} else if (args[i].equals("-outputName")) {
				outputName = args[++i];
			} else if (args[i].equals("-numQuery")) {
				numQuery = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-base")) {
				base = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-scale")) {
				scale = Double.parseDouble(args[++i]);
			} else if (args[i].equals("-rangeMinLon")) {
				rangeMinLon = Double.parseDouble(args[++i]);
			} else if (args[i].equals("-rangeMaxLon")) {
				rangeMaxLon = Double.parseDouble(args[++i]);
			} else if (args[i].equals("-rangeMinLat")) {
				rangeMinLat = Double.parseDouble(args[++i]);
			} else if (args[i].equals("-rangeMaxLat")) {
				rangeMaxLat = Double.parseDouble(args[++i]);
			}
		}

		BuildRangeQuery rq = new BuildRangeQuery(folder, numFile, dsName, outputName, numQuery, base, scale,
				rangeMinLon, rangeMaxLon, rangeMinLat, rangeMaxLat);

	}

}

class RangeQueryHelper {
	public int[] pos;
	public double val;

	public RangeQueryHelper(int[] pos, double val) {
		this.pos = pos;
		this.val = val;
	}
}
