package dataStructure.File;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import histogram.SimpleSpatialHistogramOpt;

public class LoadFile {
	
	String regex = "";

//	public int dataSizeBound;
	boolean haveSynopsis;
	public int[] synopsisType;
	public int[][] synopsisSize;

	// data range
	double MinMinLon = Double.MAX_VALUE;
	double MaxMaxLon = -Double.MAX_VALUE;
	double MinMinLat = Double.MAX_VALUE;
	double MaxMaxLat = -Double.MAX_VALUE;

	public LoadFile(int dataSizeBound, boolean haveSynopsis, String regex) {
//		this.dataSizeBound = dataSizeBound;
		this.haveSynopsis = haveSynopsis;
		this.regex = regex;
	}

	public void setSynopsis(int[] synopsisType, int[][] synopsisSize) {
		this.synopsisType = synopsisType;
		this.synopsisSize = synopsisSize;
	}

	public void buildFile(String outputPath, List<String> lines) {
		FileWriter writer;
		
//		System.out.println("MinMinLon = " + MinMinLon + ", MaxMaxLon = " + MaxMaxLon + 
//				", MinMinLat = " + MinMinLat + ", MaxMaxLat = " + MaxMaxLat);

		try {
			writer = new FileWriter(outputPath);

			double[][] dataArray = new double[lines.size()][2];
			for (int i = 0; i < lines.size(); i++) {
				String line = lines.get(i);

				// write data
				writer.write(line + "\n");

				// process data
				String[] tmp = line.split(regex);
				double lon = Double.parseDouble(tmp[0]);
				MinMinLon = Math.min(MinMinLon, lon);
				MaxMaxLon = Math.max(MaxMaxLon, lon);

				double lat = Double.parseDouble(tmp[1]);
				MinMinLat = Math.min(MinMinLat, lat);
				MaxMaxLat = Math.max(MaxMaxLat, lat);

				if (haveSynopsis) {
					double[] data = new double[2];
					data[0] = lon;
					data[1] = lat;
					dataArray[i] = data;
				}

			}
			
			MaxMaxLon += 0.000000001;
			MaxMaxLat += 0.000000001;

			int[] firstSynosisLines = new int[synopsisType.length];
			int dataLineNext = lines.size() + 1;
			// synopsis
			if (haveSynopsis) {
				int synosSize = 0;
				for (int sId = 0; sId<synopsisType.length; sId++) {
					firstSynosisLines[sId] = dataLineNext + synosSize;
					if (synopsisType[sId] == 0) {
						// build synopsis
						SimpleSpatialHistogramOpt hist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon, MaxMaxLat, 
								synopsisSize[sId][0], synopsisSize[sId][1]);
						hist = hist.loadHistByDataArray(hist, dataArray);
						
						// write synopsis header
						String synopsisHeader = hist.headerBuilder();
						writer.write(synopsisHeader);
						
						// 1. write data
						double[][] data = hist.getData();
						for (int i=0; i<data.length; i++) {
							String dataStr = Arrays.toString(data[i]) + "\n";
//							System.out.println(dataStr);
							writer.write(dataStr);
						}
						// 2. write lon boundary
						double[] lonB = hist.getLonBoundary();
						String lonBStr = Arrays.toString(lonB) + "\n";
						writer.write(lonBStr);
						// 3. write lat boundary
						double[] latB = hist.getLatBoundary();
						String latBStr = Arrays.toString(latB) + "\n";
						writer.write(latBStr);
						// 4. write frequency
						ArrayList<double[]> frequency = hist.frequency;
						for (int i=0; i<frequency.size(); i++) {
							String freStr = Arrays.toString(frequency.get(i)) + "\n";
							writer.write(freStr);
						}
						
						// synosSize = 1 + data.length + 1 + 1 + frequency.size();
						synosSize += data.length + frequency.size() + 3;
					}
				}
			}

			String tail = tailBuilder(firstSynosisLines);
			writer.write(tail);
			
			writer.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String tailBuilder(int[] firstSynosisLines) {
		String tail = MinMinLon + "," + MaxMaxLon + "," + MinMinLat + "," + MaxMaxLat + ";";
//		System.out.println(tail);
		if (haveSynopsis) {
//			tail += firstSynosisLine + "," + synopsisType + ";";
			for (int i = 0; i < firstSynosisLines.length-1; i++) {
				tail += firstSynosisLines[i] + "," + synopsisType[i] + ",";
			}
			tail += firstSynosisLines[firstSynosisLines.length-1] + "," + synopsisType[firstSynosisLines.length-1] + ";";
			for (int i = 0; i < synopsisSize.length-1; i++) {
				tail += Arrays.toString(synopsisSize[i]) + ",";
			}
			tail += Arrays.toString(synopsisSize[synopsisSize.length-1]) + "\n";
		} else {
			tail += -1 + "," + -1 + ";" + "-1" + "\n";
		}

		return tail;
	}

}
