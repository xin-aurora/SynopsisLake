package loading;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.fs.FSDataOutputStream;

import dataStructure.WaveletCoeff;
import histogram.GeometricHistogramOpt;
import histogram.SimpleSpatialHistogramOpt;
import histogram.Wavelet2D;

public class DistributedBuildFile implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -309032441501306930L;

	String regex = "";

//	public int dataSizeBound;
	boolean haveSynopsis;
	public byte[] synopsisType;
	public int[][] synopsisSize;

	// data range
	double MinMinLon = Double.MAX_VALUE;
	double MaxMaxLon = -Double.MAX_VALUE;
	double MinMinLat = Double.MAX_VALUE;
	double MaxMaxLat = -Double.MAX_VALUE;

	public DistributedBuildFile(int dataSizeBound, boolean haveSynopsis, String regex) {
//		this.dataSizeBound = dataSizeBound;
		this.haveSynopsis = haveSynopsis;
		this.regex = regex;
	}

	public void setSynopsis(byte[] synopsisType, int[][] synopsisSize) {
		this.synopsisType = synopsisType;
		this.synopsisSize = synopsisSize;
	}

	// TODO: remove read file time
	public void buildFile(FSDataOutputStream writer, List<String> lines) {

		if (synopsisType[0] == 1) {
			buildGHFile(writer, lines);
		} else {
			try {
				int dataSize = 0;
				byte recordDimension = 0;
				double[][] dataArray = new double[lines.size()][2];
				for (int i = 1; i < lines.size(); i++) {
					String line = lines.get(i) + "\n";

					// process data
					String[] tmp = line.split(regex);
					double lon = Double.parseDouble(tmp[0]);
					MinMinLon = Math.min(MinMinLon, lon);
					MaxMaxLon = Math.max(MaxMaxLon, lon);

					double lat = Double.parseDouble(tmp[1]);
					MinMinLat = Math.min(MinMinLat, lat);
					MaxMaxLat = Math.max(MaxMaxLat, lat);

					if (tmp.length == 2) {
						// write data

						Record r = new Record(lon, lat, 0, new byte[0]);
						writer.write(r.getData());
						dataSize += r.getLen();
						recordDimension = 2;
					}
					if (tmp.length == 3) {
						// write data
						byte[] attributeByte = tmp[2].getBytes(StandardCharsets.UTF_8);
						Record r = new Record(lon, lat, attributeByte.length, attributeByte);
						writer.write(r.getData());
						dataSize += r.getLen();
						recordDimension = 3;
					}

					if (haveSynopsis) {
						double[] data = new double[2];
						data[0] = lon;
						data[1] = lat;
						dataArray[i] = data;
					}

				}

				// TODO: EOF mark, dataSize += EOF_Len

				MaxMaxLon += 0.000000001;
				MaxMaxLat += 0.000000001;

				int[] firstSynosisLines = new int[synopsisType.length];
				// synopsis
				if (haveSynopsis) {
					int synosSize = 0;
					for (int sId = 0; sId < synopsisType.length; sId++) {
						firstSynosisLines[sId] = dataSize + synosSize;
						if (synopsisType[sId] == 0) {
							// build synopsis
							SimpleSpatialHistogramOpt hist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat,
									MaxMaxLon, MaxMaxLat, synopsisSize[sId][0], synopsisSize[sId][1]);
							hist = hist.loadHistByDataArray(hist, dataArray);

							// write synopsis header
							byte[] synopsisHeader = hist.byteHeaderBuilder();
							writer.write(synopsisHeader);

							synosSize += synopsisHeader.length;

							// 1. write data
							double[][] data = hist.getData();
							int lonSize = data.length;
							int latSize = data[0].length;
							ByteBuffer dataBuffer = ByteBuffer.allocate(8 * lonSize * latSize);
							int dataOffset = 0;
							// 2. write lon boundary
							ByteBuffer lonBuffer = ByteBuffer.allocate(8 * lonSize);
							double[] lonB = hist.getLonBoundary();
							int lonOffset = 0;
							// 3. write lat boundary
							ByteBuffer latBuffer = ByteBuffer.allocate(8 * latSize);
							double[] latB = hist.getLatBoundary();
							int latOffset = 0;
							// 4. write frequency
							ByteBuffer freLonBuffer = ByteBuffer.allocate(8 * lonSize);
							ByteBuffer freLatBuffer = ByteBuffer.allocate(8 * latSize);
							ArrayList<double[]> frequency = hist.frequency;
							int freLonOffset = 0;
							int freLatOffset = 0;
							for (int lon = 0; lon < data.length; lon++) {
								lonBuffer.putDouble(lonOffset, lonB[lon]);
								lonOffset += 8;
								freLonBuffer.putDouble(freLonOffset, frequency.get(0)[lon]);
								freLonOffset += 8;
								for (int lat = 0; lat < data[0].length; lat++) {
									if (lat == 0) {
										latBuffer.putDouble(latOffset, latB[lat]);
										latOffset += 8;
										freLatBuffer.putDouble(freLatOffset, frequency.get(1)[lat]);
										freLatOffset += 8;
									}
									dataBuffer.putDouble(dataOffset, data[lon][lat]);
									dataOffset += 8;
								}
							}

							// synosSize = header + data + lon + lat + frequency
							synosSize = synopsisHeader.length + 8 * lonSize * latSize + 16 * lonSize + 16 * latSize;
//							synosSize = synopsisHeader.length + 8 * lonSize * latSize + 8 * lonSize + 8 * latSize;
							writer.write(dataBuffer.array());
							writer.write(lonBuffer.array());
							writer.write(latBuffer.array());
							writer.write(freLonBuffer.array());
							writer.write(freLatBuffer.array());
						} else if (synopsisType[sId] == 2) {
							// build synopsis
							SimpleSpatialHistogramOpt hist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat,
									MaxMaxLon, MaxMaxLat, synopsisSize[sId][0], synopsisSize[sId][1]);
							hist = hist.loadHistByDataArray(hist, dataArray);

							Wavelet2D wavelet = new Wavelet2D(hist);

							// write synopsis header
							byte[] synopsisHeader = hist.byteHeaderBuilder();
							writer.write(synopsisHeader);

							synosSize += synopsisHeader.length;

							// 1. write coeffArrays
							WaveletCoeff[][] coeffArrays = wavelet.getCoeffArray();
							for (int i = 0; i < coeffArrays.length; i++) {
								for (int j = 0; j < coeffArrays[0].length; j++) {
									byte[] coeffBuffer = coeffArrays[i][j].getByteArray();
									synosSize += coeffBuffer.length;
									writer.write(coeffBuffer);
								}
							}
							
							// 2. write lon boundary
							double[] lonB = hist.getLonBoundary();
							int lonSize = lonB.length;
							ByteBuffer lonBuffer = ByteBuffer.allocate(8 * lonSize);
							int lonOffset = 0;
							// 3. write lat boundary
							double[] latB = hist.getLatBoundary();
							int latSize = latB.length;
							ByteBuffer latBuffer = ByteBuffer.allocate(8 * latSize);
							int latOffset = 0;
							// 4. write frequency
							ByteBuffer freLonBuffer = ByteBuffer.allocate(8 * lonSize);
							ByteBuffer freLatBuffer = ByteBuffer.allocate(8 * latSize);
							ArrayList<double[]> frequency = hist.frequency;
							int freLonOffset = 0;
							int freLatOffset = 0;
							// write lon and lon frequency
							for (int lon = 0; lon < lonSize; lon++) {
								lonBuffer.putDouble(lonOffset, lonB[lon]);
								lonOffset += 8;
								freLonBuffer.putDouble(freLonOffset, frequency.get(0)[lon]);
								freLonOffset += 8;
								
							}
							// write lat and lat frequency
							for (int lat = 0; lat < latSize; lat++) {
								if (lat == 0) {
									latBuffer.putDouble(latOffset, latB[lat]);
									latOffset += 8;
									freLatBuffer.putDouble(freLatOffset, frequency.get(1)[lat]);
									freLatOffset += 8;
								}

							}
							
							synosSize += 16 * lonSize + 16 * latSize;
							writer.write(lonBuffer.array());
							writer.write(latBuffer.array());
							writer.write(freLonBuffer.array());
							writer.write(freLatBuffer.array());
						}
					}
				}

				byte[] tail = tailBuilder(firstSynosisLines, recordDimension);
				writer.write(tail);
				
				writer.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void buildFile(FileOutputStream writer, List<String> lines) {
		try {

			int dataSize = 0;
			byte recordDimension = 0;
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

				if (tmp.length == 2) {
					Record r = new Record(lon, lat, 0, new byte[0]);
					writer.write(r.getData());
					dataSize += r.getLen();
					recordDimension = 2;
				}
				if (tmp.length == 3) {
					byte[] attributeByte = tmp[2].getBytes(StandardCharsets.UTF_8);
					Record r = new Record(lon, lat, attributeByte.length, attributeByte);
					writer.write(r.getData());
					dataSize += r.getLen();
					recordDimension = 3;
				}

				if (haveSynopsis) {
					double[] data = new double[2];
					data[0] = lon;
					data[1] = lat;
					dataArray[i] = data;
				}

			}

			// TODO: EOF mark, dataSize += EOF_Len

			MaxMaxLon += 0.000000001;
			MaxMaxLat += 0.000000001;

			int[] firstSynosisLines = new int[synopsisType.length];
			// synopsis
			if (haveSynopsis) {
				int synosSize = 0;
				for (int sId = 0; sId < synopsisType.length; sId++) {
					firstSynosisLines[sId] = dataSize + synosSize;
					if (synopsisType[sId] == 0) {
						// build synopsis
						SimpleSpatialHistogramOpt hist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
								MaxMaxLat, synopsisSize[sId][0], synopsisSize[sId][1]);
						hist = hist.loadHistByDataArray(hist, dataArray);

						// write synopsis header
						byte[] synopsisHeader = hist.byteHeaderBuilder();
						writer.write(synopsisHeader);

						synosSize += synopsisHeader.length;

						// 1. write data
						double[][] data = hist.getData();
						int lonSize = data.length;
						int latSize = data[0].length;
						ByteBuffer dataBuffer = ByteBuffer.allocate(8 * lonSize * latSize);
						int dataOffset = 0;
						// 2. write lon boundary
						ByteBuffer lonBuffer = ByteBuffer.allocate(8 * lonSize);
						double[] lonB = hist.getLonBoundary();
						int lonOffset = 0;
						// 3. write lat boundary
						ByteBuffer latBuffer = ByteBuffer.allocate(8 * latSize);
						double[] latB = hist.getLatBoundary();
						int latOffset = 0;
						// 4. write frequency
						ByteBuffer freLonBuffer = ByteBuffer.allocate(8 * lonSize);
						ByteBuffer freLatBuffer = ByteBuffer.allocate(8 * latSize);
						ArrayList<double[]> frequency = hist.frequency;
						int freLonOffset = 0;
						int freLatOffset = 0;
						for (int lon = 0; lon < data.length; lon++) {
							lonBuffer.putDouble(lonOffset, lonB[lon]);
							lonOffset += 8;
							freLonBuffer.putDouble(freLonOffset, frequency.get(0)[lon]);
							freLonOffset += 8;
							for (int lat = 0; lat < data[0].length; lat++) {
								if (lat == 0) {
									latBuffer.putDouble(latOffset, latB[lat]);
									latOffset += 8;
									freLatBuffer.putDouble(freLatOffset, frequency.get(1)[lat]);
									freLatOffset += 8;
								}
								dataBuffer.putDouble(dataOffset, data[lon][lat]);
								dataOffset += 8;
							}
						}

						// synosSize = header + data + lon + lat + frequency
						synosSize = synopsisHeader.length + 8 * lonSize * latSize + 16 * lonSize + 16 * latSize;
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

			byte[] tail = tailBuilder(firstSynosisLines, recordDimension);
			writer.write(tail);

			writer.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void buildGHFile(FSDataOutputStream writer, List<String> lines) {

		try {

			int dataSize = 0;
			byte recordDimension = 4;
			double[][] dataArray = new double[lines.size()][4];
			for (int i = 1; i < lines.size(); i++) {
				String line = lines.get(i) + "\n";

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

				String att = "";
				for (int attId = 4; attId < tmp.length; attId++) {
					att += tmp[attId] + ";";
				}
				byte[] attributeByte = att.getBytes(StandardCharsets.UTF_8);

				Record r = new Record(minLon, minLat, maxLon, maxLat, attributeByte.length, attributeByte);
				writer.write(r.getData());
				dataSize += r.getLen();

				if (haveSynopsis) {
					double[] data = new double[4];
					data[0] = minLon;
					data[1] = minLat;
					data[2] = maxLon;
					data[3] = maxLat;
					dataArray[i] = data;
				}

			}

			// TODO: EOF mark, dataSize += EOF_Len

			MaxMaxLon += 0.000000001;
			MaxMaxLat += 0.000000001;

			int[] firstSynosisLines = new int[synopsisType.length];
			// Geometric Histogram synopsis
			if (haveSynopsis) {
				int synosSize = 0;
				for (int sId = 0; sId < synopsisType.length; sId++) {
					firstSynosisLines[sId] = dataSize + synosSize;

					GeometricHistogramOpt hist = new GeometricHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon, MaxMaxLat,
							synopsisSize[sId][0], synopsisSize[sId][1]);
					hist = hist.loadHistByDataArray(hist, dataArray);

					// write synopsis header
					byte[] synopsisHeader = hist.byteHeaderBuilder();
					writer.write(synopsisHeader);

					synosSize += synopsisHeader.length;

					// 1. write data
					double[][] count = hist.getCount();
					double[][] area = hist.getArea();
					double[][] ratioH = hist.getRatioH();
					double[][] ratioV = hist.getRatioV();
					int lonSize = count.length;
					int latSize = count[0].length;
					ByteBuffer countBuffer = ByteBuffer.allocate(8 * lonSize * latSize);
					ByteBuffer areaBuffer = ByteBuffer.allocate(8 * lonSize * latSize);
					ByteBuffer ratioHBuffer = ByteBuffer.allocate(8 * lonSize * latSize);
					ByteBuffer ratioVBuffer = ByteBuffer.allocate(8 * lonSize * latSize);
					int dataOffset = 0;
					// 2. write lon boundary
					ByteBuffer lonBuffer = ByteBuffer.allocate(8 * lonSize);
					double[] lonB = hist.getLonBoundary();
					int lonOffset = 0;
					// 3. write lat boundary
					ByteBuffer latBuffer = ByteBuffer.allocate(8 * latSize);
					double[] latB = hist.getLatBoundary();
					int latOffset = 0;
					// 4. write frequency
					ByteBuffer freLonBuffer = ByteBuffer.allocate(8 * lonSize);
					ByteBuffer freLatBuffer = ByteBuffer.allocate(8 * latSize);
					ArrayList<double[]> frequency = hist.frequency;
					int freLonOffset = 0;
					int freLatOffset = 0;
					// fill buffer
					for (int lon = 0; lon < count.length; lon++) {
						lonBuffer.putDouble(lonOffset, lonB[lon]);
						lonOffset += 8;
						freLonBuffer.putDouble(freLonOffset, frequency.get(0)[lon]);
						freLonOffset += 8;
						for (int lat = 0; lat < count[0].length; lat++) {
							if (lat == 0) {
								latBuffer.putDouble(latOffset, latB[lat]);
								latOffset += 8;
								freLatBuffer.putDouble(freLatOffset, frequency.get(1)[lat]);
								freLatOffset += 8;
							}
							countBuffer.putDouble(dataOffset, count[lon][lat]);
							areaBuffer.putDouble(dataOffset, area[lon][lat]);
							ratioHBuffer.putDouble(dataOffset, ratioH[lon][lat]);
							ratioVBuffer.putDouble(dataOffset, ratioV[lon][lat]);
							dataOffset += 8;
						}
					}

					// synosSize = header + data + lon + lat + frequency
					synosSize = synopsisHeader.length + 32 * lonSize * latSize + 16 * lonSize + 16 * latSize;
//					synosSize = synopsisHeader.length + 32 * lonSize * latSize + 8 * lonSize + 8 * latSize;
					writer.write(countBuffer.array());
					writer.write(areaBuffer.array());
					writer.write(ratioHBuffer.array());
					writer.write(ratioVBuffer.array());
					writer.write(lonBuffer.array());
					writer.write(latBuffer.array());
					writer.write(freLonBuffer.array());
					writer.write(freLatBuffer.array());

				}
			}

			byte[] tail = tailBuilder(firstSynosisLines, recordDimension);
			writer.write(tail);

			writer.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public byte[] tailBuilder(int[] firstSynosisLines, byte recordDimension) {
		// compute length of tail
		// MinMinLon, MinMinLat, MaxMaxLon, MaxMaxLat = Double.BYTES*4
		// for each synopsis: start position of the synopsis (int), synopsis type (byte)
		// = 4 bytes + 1 byte
		ByteBuffer byteBuffer = null;

		if (haveSynopsis) {
			int tailSize = 1 + 32 + firstSynosisLines.length * 5 + 4;
			byteBuffer = ByteBuffer.allocate(tailSize);
			byteBuffer.put(recordDimension);
			byteBuffer.putDouble(1, MinMinLon);
			byteBuffer.putDouble(9, MinMinLat);
			byteBuffer.putDouble(17, MaxMaxLon);
			byteBuffer.putDouble(25, MaxMaxLat);
			int curIdx = 33;
			for (int i = 0; i < firstSynosisLines.length; i++) {
				byteBuffer.putInt(curIdx, firstSynosisLines[i]);
				curIdx += 4;
				byteBuffer.put(curIdx, synopsisType[i]);
				curIdx += 1;
			}
			byteBuffer.putInt(curIdx, tailSize);
		} else {
			// tail size = the size of the four boundaries
			// 1 + 32 + 4 = 37
			int tailSize = 37;
			byteBuffer = ByteBuffer.allocate(tailSize);
			byteBuffer.put(recordDimension);
			byteBuffer.putDouble(1, MinMinLon);
			byteBuffer.putDouble(9, MinMinLat);
			byteBuffer.putDouble(17, MaxMaxLon);
			byteBuffer.putDouble(25, MaxMaxLat);
			byteBuffer.putInt(33, tailSize);
		}

		return byteBuffer.array();
	}
}
