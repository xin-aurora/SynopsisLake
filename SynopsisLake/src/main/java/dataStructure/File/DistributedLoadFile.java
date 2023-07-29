package dataStructure.File;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;

import dataStructure.WaveletCoeff;
import histogram.GeometricHistogramOpt;
import histogram.SimpleSpatialHistogramOpt;
import histogram.Wavelet2D;

public class DistributedLoadFile implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -309032441501306930L;

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

	public DistributedLoadFile(int dataSizeBound, boolean haveSynopsis, String regex) {
//		this.dataSizeBound = dataSizeBound;
		this.haveSynopsis = haveSynopsis;
		this.regex = regex;
	}

	public void setSynopsis(int[] synopsisType, int[][] synopsisSize) {
		this.synopsisType = synopsisType;
		this.synopsisSize = synopsisSize;
	}

	public void buildFile(FileWriter writer, ArrayList<String> lines) {

//		System.out.println("MinMinLon = " + MinMinLon + ", MaxMaxLon = " + MaxMaxLon + 
//				", MinMinLat = " + MinMinLat + ", MaxMaxLat = " + MaxMaxLat);
//		System.out.println("In DistributedLoadFile");
//		System.out.println("synopsis type = " + synopsisType[0]);
		if (haveSynopsis && synopsisType[0] == 1) {
			buildGHFile(writer, lines);
		} else {
			try {

				double[][] dataArray = new double[lines.size()][2];
				for (int i = 1; i < lines.size(); i++) {
					String line = lines.get(i) + "\n";


					// process data
					String[] tmp = line.split(regex);
					if (tmp.length < 3) {
						continue;
					}
					
					// write data
					writer.write(line);
					
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
					for (int sId = 0; sId < synopsisType.length; sId++) {
						firstSynosisLines[sId] = dataLineNext + synosSize;
						if (synopsisType[sId] == 0) {
							// build synopsis
							SimpleSpatialHistogramOpt hist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
									MaxMaxLat, synopsisSize[sId][0], synopsisSize[sId][1]);
							hist = hist.loadHistByDataArray(hist, dataArray);

							// write synopsis header
							String synopsisHeader = hist.headerBuilder();
							writer.write(synopsisHeader);

							// 1. write data
							double[][] data = hist.getData();
							for (int i = 0; i < data.length; i++) {
								String dataStr = Arrays.toString(data[i]) + "\n";
//								System.out.println(dataStr);
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
							for (int i = 0; i < frequency.size(); i++) {
								String freStr = Arrays.toString(frequency.get(i)) + "\n";
								writer.write(freStr);
							}

							// synosSize = 1 + data.length + 1 + 1 + frequency.size();
							synosSize += data.length + frequency.size() + 3;
						} else if (synopsisType[sId] == 1) {
							// build synopsis
							GeometricHistogramOpt hist = new GeometricHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
									MaxMaxLat, synopsisSize[sId][0], synopsisSize[sId][1]);
							hist = hist.loadHistByDataArray(hist, dataArray);

							// write synopsis header
							String synopsisHeader = hist.headerBuilder();
							writer.write(synopsisHeader);

							// 1. write data
							double[][] count = hist.getCount();
							for (int i = 0; i < count.length; i++) {
								String dataStr = Arrays.toString(count[i]) + "\n";
//								System.out.println(dataStr);
								writer.write(dataStr);
							}
							// 2. write area
							double[][] area = hist.getArea();
							for (int i = 0; i < area.length; i++) {
								String dataStr = Arrays.toString(area[i]) + "\n";
//								System.out.println(dataStr);
								writer.write(dataStr);
							}
							// 3. write ratioH
							double[][] ratioH = hist.getRatioH();
							for (int i = 0; i < ratioH.length; i++) {
								String dataStr = Arrays.toString(ratioH[i]) + "\n";
//								System.out.println(dataStr);
								writer.write(dataStr);
							}
							// 1. write ratioV
							double[][] ratioV = hist.getRatioV();
							for (int i = 0; i < ratioV.length; i++) {
								String dataStr = Arrays.toString(ratioV[i]) + "\n";
//								System.out.println(dataStr);
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
							for (int i = 0; i < frequency.size(); i++) {
								String freStr = Arrays.toString(frequency.get(i)) + "\n";
								writer.write(freStr);
							}

							// synosSize = 1 + data.length + 1 + 1 + frequency.size();
							synosSize += count.length + area.length + ratioH.length + ratioV.length + frequency.size() + 3;
						} else if (synopsisType[sId] == 2) {
							// build synopsis
							SimpleSpatialHistogramOpt hist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
									MaxMaxLat, synopsisSize[sId][0], synopsisSize[sId][1]);
							hist = hist.loadHistByDataArray(hist, dataArray);

							Wavelet2D wavelet = new Wavelet2D(hist);

							// write synopsis header
							String synopsisHeader = hist.headerBuilder();
							writer.write(synopsisHeader);

							// 1. write coeffArrays
							WaveletCoeff[][] coeffArrays = wavelet.getCoeffArray();
							for (int i = 0; i < coeffArrays.length; i++) {
								String dataStr = Arrays.toString(coeffArrays[i]) + "\n";
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
							for (int i = 0; i < frequency.size(); i++) {
								String freStr = Arrays.toString(frequency.get(i)) + "\n";
								writer.write(freStr);
							}

							// synosSize = 1 + data.length + 1 + 1 + frequency.size();
							synosSize += coeffArrays.length + frequency.size() + 3;
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

		
	}

	public void buildFile(FSDataOutputStream writer, List<String> lines) {

//		System.out.println("MinMinLon = " + MinMinLon + ", MaxMaxLon = " + MaxMaxLon + 
//				", MinMinLat = " + MinMinLat + ", MaxMaxLat = " + MaxMaxLat);

		if (haveSynopsis && synopsisType[0] == 1) {
			buildGHFile(writer, lines);
		} else {
			try {

				double[][] dataArray = new double[lines.size()][2];
				for (int i = 1; i < lines.size(); i++) {
					String line = lines.get(i) + "\n";

					// write data
					writer.write(line.getBytes());

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
					for (int sId = 0; sId < synopsisType.length; sId++) {
						firstSynosisLines[sId] = dataLineNext + synosSize;
						if (synopsisType[sId] == 0) {
							// build synopsis
							SimpleSpatialHistogramOpt hist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
									MaxMaxLat, synopsisSize[sId][0], synopsisSize[sId][1]);
							hist = hist.loadHistByDataArray(hist, dataArray);

							// write synopsis header
							String synopsisHeader = hist.headerBuilder();
							writer.write(synopsisHeader.getBytes());

							// 1. write data
							double[][] data = hist.getData();
							for (int i = 0; i < data.length; i++) {
								String dataStr = Arrays.toString(data[i]) + "\n";
//								System.out.println(dataStr);
								writer.write(dataStr.getBytes());
							}
							// 2. write lon boundary
							double[] lonB = hist.getLonBoundary();
							String lonBStr = Arrays.toString(lonB) + "\n";
							writer.write(lonBStr.getBytes());
							// 3. write lat boundary
							double[] latB = hist.getLatBoundary();
							String latBStr = Arrays.toString(latB) + "\n";
							writer.write(latBStr.getBytes());
							// 4. write frequency
							ArrayList<double[]> frequency = hist.frequency;
							for (int i = 0; i < frequency.size(); i++) {
								String freStr = Arrays.toString(frequency.get(i)) + "\n";
								writer.write(freStr.getBytes());
							}

							// synosSize = 1 + data.length + 1 + 1 + frequency.size();
							synosSize += data.length + frequency.size() + 3;
						} else if (synopsisType[sId] == 2) {
							// build synopsis
							SimpleSpatialHistogramOpt hist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
									MaxMaxLat, synopsisSize[sId][0], synopsisSize[sId][1]);
							hist = hist.loadHistByDataArray(hist, dataArray);

							Wavelet2D wavelet = new Wavelet2D(hist);

							// write synopsis header
							String synopsisHeader = hist.headerBuilder();
							writer.write(synopsisHeader.getBytes());

							// 1. write coeffArrays
							WaveletCoeff[][] coeffArrays = wavelet.getCoeffArray();
							for (int i = 0; i < coeffArrays.length; i++) {
								String dataStr = Arrays.toString(coeffArrays[i]) + "\n";
								writer.write(dataStr.getBytes());
							}

							// 2. write lon boundary
							double[] lonB = hist.getLonBoundary();
							String lonBStr = Arrays.toString(lonB) + "\n";
							writer.write(lonBStr.getBytes());
							// 3. write lat boundary
							double[] latB = hist.getLatBoundary();
							String latBStr = Arrays.toString(latB) + "\n";
							writer.write(latBStr.getBytes());
							// 4. write frequency
							ArrayList<double[]> frequency = hist.frequency;
							for (int i = 0; i < frequency.size(); i++) {
								String freStr = Arrays.toString(frequency.get(i)) + "\n";
								writer.write(freStr.getBytes());
							}

							// synosSize = 1 + data.length + 1 + 1 + frequency.size();
							synosSize += coeffArrays.length + frequency.size() + 3;
						}
					}
				}

				String tail = tailBuilder(firstSynosisLines);
				writer.write(tail.getBytes());

				writer.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		
	}

	public void buildGHFile(FSDataOutputStream writer, List<String> lines) {

//		System.out.println("MinMinLon = " + MinMinLon + ", MaxMaxLon = " + MaxMaxLon + 
//				", MinMinLat = " + MinMinLat + ", MaxMaxLat = " + MaxMaxLat);
		try {

			double[][] dataArray = new double[lines.size()][4];
			for (int i = 1; i < lines.size(); i++) {
				String line = lines.get(i) + "\n";

				// write data
				writer.write(line.getBytes());

				// process data
				String[] tmp = line.split(regex);
				double minLon = Double.parseDouble(tmp[0]);

				double minLat = Double.parseDouble(tmp[1]);

				double maxLon = Double.parseDouble(tmp[2]);
				MinMinLon = Math.min(MinMinLon, minLon);
				MaxMaxLon = Math.max(MaxMaxLon, maxLon);

				double maxLat = Double.parseDouble(tmp[3]);
				MinMinLat = Math.min(MinMinLat, minLat);
				MaxMaxLat = Math.max(MaxMaxLat, maxLat);

				if (haveSynopsis) {
					double[] data = new double[4];
					data[0] = minLon;
					data[1] = minLat;
					data[2] = maxLon;
					data[3] = maxLat;
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
				for (int sId = 0; sId < synopsisType.length; sId++) {
					firstSynosisLines[sId] = dataLineNext + synosSize;

					// build synopsis
					GeometricHistogramOpt hist = new GeometricHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon, MaxMaxLat,
							synopsisSize[sId][0], synopsisSize[sId][1]);
					hist = hist.loadHistByDataArray(hist, dataArray);

					// write synopsis header
					String synopsisHeader = hist.headerBuilder();
					writer.write(synopsisHeader.getBytes());

					// 1. write data
					double[][] count = hist.getCount();
					for (int i = 0; i < count.length; i++) {
						String dataStr = Arrays.toString(count[i]) + "\n";
//								System.out.println(dataStr);
						writer.write(dataStr.getBytes());
					}
					// 2. write area
					double[][] area = hist.getArea();
					for (int i = 0; i < area.length; i++) {
						String dataStr = Arrays.toString(area[i]) + "\n";
//								System.out.println(dataStr);
						writer.write(dataStr.getBytes());
					}
					// 3. write ratioH
					double[][] ratioH = hist.getRatioH();
					for (int i = 0; i < ratioH.length; i++) {
						String dataStr = Arrays.toString(ratioH[i]) + "\n";
//								System.out.println(dataStr);
						writer.write(dataStr.getBytes());
					}
					// 1. write ratioV
					double[][] ratioV = hist.getRatioV();
					for (int i = 0; i < ratioV.length; i++) {
						String dataStr = Arrays.toString(ratioV[i]) + "\n";
//								System.out.println(dataStr);
						writer.write(dataStr.getBytes());
					}
					// 2. write lon boundary
					double[] lonB = hist.getLonBoundary();
					String lonBStr = Arrays.toString(lonB) + "\n";
					writer.write(lonBStr.getBytes());
					// 3. write lat boundary
					double[] latB = hist.getLatBoundary();
					String latBStr = Arrays.toString(latB) + "\n";
					writer.write(latBStr.getBytes());
					// 4. write frequency
					ArrayList<double[]> frequency = hist.frequency;
					for (int i = 0; i < frequency.size(); i++) {
						String freStr = Arrays.toString(frequency.get(i)) + "\n";
						writer.write(freStr.getBytes());
					}

					// synosSize = 1 + data.length + 1 + 1 + frequency.size();
					synosSize += count.length + area.length + ratioH.length + ratioV.length + frequency.size() + 3;

				}
			}

			String tail = tailBuilder(firstSynosisLines);
			writer.write(tail.getBytes());

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
			for (int i = 0; i < firstSynosisLines.length - 1; i++) {
				tail += firstSynosisLines[i] + "," + synopsisType[i] + ",";
			}
			tail += firstSynosisLines[firstSynosisLines.length - 1] + "," + synopsisType[firstSynosisLines.length - 1]
					+ ";";
			for (int i = 0; i < synopsisSize.length - 1; i++) {
				tail += Arrays.toString(synopsisSize[i]) + ",";
			}
			tail += Arrays.toString(synopsisSize[synopsisSize.length - 1]) + "\n";
		} else {
			tail += -1 + "," + -1 + ";" + "-1" + "\n";
		}

		return tail;
	}

	public void buildGHFile(FileWriter writer, ArrayList<String> lines) {

		try {
			
			double[][] dataArray = new double[lines.size()][4];
//			System.out.println("lines size = " + lines.size());
			for (int i = 1; i < lines.size(); i++) {
				String line = lines.get(i) + "\n";

				// write data
				writer.write(line);

				// process data
				String[] tmp = line.split(regex);
				double minLon = Double.parseDouble(tmp[0]);

				double minLat = Double.parseDouble(tmp[1]);

				double maxLon = Double.parseDouble(tmp[2]);
				MinMinLon = Math.min(MinMinLon, minLon);
				MaxMaxLon = Math.max(MaxMaxLon, maxLon);

				double maxLat = Double.parseDouble(tmp[3]);
				MinMinLat = Math.min(MinMinLat, minLat);
				MaxMaxLat = Math.max(MaxMaxLat, maxLat);

				if (haveSynopsis) {
					double[] data = new double[4];
					data[0] = minLon;
					data[1] = minLat;
					data[2] = maxLon;
					data[3] = maxLat;
					dataArray[i] = data;
				}

			}

			MaxMaxLon += 0.000000001;
			MaxMaxLat += 0.000000001;

			int[] firstSynosisLines = new int[synopsisType.length];
			int dataLineNext = lines.size();
			// synopsis
			if (haveSynopsis) {
				int synosSize = 0;
				for (int sId = 0; sId < synopsisType.length; sId++) {
					firstSynosisLines[sId] = dataLineNext + synosSize;

					// build synopsis
					GeometricHistogramOpt hist = new GeometricHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon, MaxMaxLat,
							synopsisSize[sId][0], synopsisSize[sId][1]);
					hist = hist.loadHistByDataArray(hist, dataArray);

					// write synopsis header
					String synopsisHeader = hist.headerBuilder();
//					System.out.println("synopsisHeader = " + synopsisHeader);
					writer.write(synopsisHeader);

					// 1. write data
					double[][] count = hist.getCount();
					for (int i = 0; i < count.length; i++) {
						String dataStr = Arrays.toString(count[i]) + "\n";
//							System.out.println(dataStr);
						writer.write(dataStr);
					}
					// 2. write area
					double[][] area = hist.getArea();
					for (int i = 0; i < area.length; i++) {
						String dataStr = Arrays.toString(area[i]) + "\n";
//							System.out.println(dataStr);
						writer.write(dataStr);
					}
					// 3. write ratioH
					double[][] ratioH = hist.getRatioH();
					for (int i = 0; i < ratioH.length; i++) {
						String dataStr = Arrays.toString(ratioH[i]) + "\n";
//							System.out.println(dataStr);
						writer.write(dataStr);
					}
					// 1. write ratioV
					double[][] ratioV = hist.getRatioV();
					for (int i = 0; i < ratioV.length; i++) {
						String dataStr = Arrays.toString(ratioV[i]) + "\n";
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
					for (int i = 0; i < frequency.size(); i++) {
						String freStr = Arrays.toString(frequency.get(i)) + "\n";
						writer.write(freStr);
					}

					// synosSize = header:1 + data.length + lon-1 + lat-1 + frequency.size();
					synosSize += count.length + area.length + ratioH.length + ratioV.length + frequency.size() + 3;
//					System.out.println("synosSize = " + synosSize);
				}
			}

			String tail = tailBuilder(firstSynosisLines);
//			System.out.println("tail = " + tail);
			writer.write(tail);

			writer.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
