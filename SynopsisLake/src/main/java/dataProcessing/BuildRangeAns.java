package dataProcessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BuildRangeAns {
	
	String folder = "";
	String queryName = "";
	int numFile = 0;
	String dsName = "";
	String regex = "";
	String outputName = "";
	public BuildRangeAns(String folder, String queryName, int numFile, String dsName, String outputName) {
		
		this.folder = folder;
		this.queryName = queryName;
		this.numFile = numFile;
		this.dsName = dsName;
		if (dsName.startsWith("syn")) {
			regex = ",";
		} else {
			regex = "\t";
		}
		
		this.outputName = outputName;
		
		// step 1: load all query
		ArrayList<double[]> queries = readQueries();
		
		// step 2: query ans
		// for each data point, go over queries
		// if in query range, ans+=1
		queries = readData(queries);
		
		// step 3: write ans
		writeAns(queries);
	}
	
	public void writeAns(ArrayList<double[]> queries) {
		FileWriter myWriter;
		String ansPath = folder + outputName;
		try {
			myWriter = new FileWriter(ansPath, false);
			System.out.println("query path = " + ansPath);
//			int cnt = 0;
			for (int qId = 0; qId < queries.size(); qId++) {
				double minLon = queries.get(qId)[0];
				double minLat = queries.get(qId)[1];
				double maxLon = queries.get(qId)[2];
				double maxLat = queries.get(qId)[3];
				double ans = queries.get(qId)[4];
				String query = minLon + "," + minLat + "," + maxLon + "," + maxLat + "," + ans + "\n";
				myWriter.write(query);

//				System.out.println(Arrays.toString(queries.get(qId)));
			}
			
//			System.out.println("cnt = " + cnt);

			myWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public ArrayList<double[]> readData(ArrayList<double[]> queries){
		
		for (int fileId=0; fileId < numFile; fileId++) {
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
					double posLon = Double.parseDouble(tmp[0]);
					double posLat = Double.parseDouble(tmp[1]);
					double ans = Double.parseDouble(tmp[3]);
					for (int qId=0; qId < queries.size(); qId++) {
//					for (int qId=0; qId < 10; qId++) {
						double minLon = queries.get(qId)[0];
						double minLat = queries.get(qId)[1];
						double maxLon = queries.get(qId)[2];
						double maxLat = queries.get(qId)[3];
						if (minLon <= posLon && posLon <= maxLon && minLat <= posLat && posLat <= maxLat) {
//							queries.get(qId)[4]++;
							queries.get(qId)[4]+= ans;
						}
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return queries;
	}
	
	public ArrayList<double[]> readQueries(){
		ArrayList<double[]> queries = new ArrayList<double[]>();
		
		String queryPath = folder + queryName;
		
		FileReader fr;
		try {
			fr = new FileReader(queryPath);
			BufferedReader br = new BufferedReader(fr);
			String line = br.readLine();
			while (line != null) {
				String[] tem = line.split(",");
				double[] query = new double[5];
				// minlon, minlat, maxlon, maxlat, ans
				query[0] = Double.parseDouble(tem[0]);
				query[1] = Double.parseDouble(tem[1]);
				query[2] = Double.parseDouble(tem[2]);
				query[3] = Double.parseDouble(tem[3]);
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
		
		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/"
				+ "Spatial/sketches/data/medium/point/";
		String queryName = "syn-3-1024RQ_sample.txt";
		int numFile = 2;
//		String dsName = "wildfire_shuffle/";
		String dsName = "synthetic/";
		String outputName = "syn-3-1024RQ_sample_ans.txt";
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-numFile")) {
				numFile = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-folder")) {
				folder = args[++i];
			} else if (args[i].equals("-queryName")) {
				queryName = args[++i];
			} else if (args[i].equals("-dsName")) {
				dsName = args[++i];
			} else if (args[i].equals("-outputName")) {
				outputName = args[++i];
			}
		}
		
		BuildRangeAns ans = new BuildRangeAns(folder, queryName, numFile, dsName, outputName);
		
	}

}
