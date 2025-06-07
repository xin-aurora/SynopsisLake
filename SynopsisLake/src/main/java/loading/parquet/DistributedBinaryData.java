package loading.parquet;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;

import dataStructure.parquet.ParquetRow;
import dataStructure.parquet.ParquetRow2DCountHistogram;
import dataStructure.parquet.ParquetRow2DWavelets;
import dataStructure.parquet.ParquetRowGeometricHistogram;
import dataStructure.parquet.ParquetRowSpatialSketch;
import dataStructure.parquet.ParquetRowUniformSamples;
import histogram.GeometricHistogramOpt;
import histogram.SimpleSpatialHistogramOpt;
import histogram.Wavelet2D;
import samples.UniformSamples;
import sketch.DyadicSpatialSketch;

public class DistributedBinaryData implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5682327926521484104L;

	String regex = "";

	public int id;
	boolean haveSynopsis;
	public int synopsisType;
	public int[] synopsisSize;
	public double uniformSampleRate;

	public long totalFileSizeInBytes = 0;

	// data range
	double MinMinLon = Double.MAX_VALUE;
	double MaxMaxLon = -Double.MAX_VALUE;
	double MinMinLat = Double.MAX_VALUE;
	double MaxMaxLat = -Double.MAX_VALUE;

	// Parquet row
	ParquetRow row;

	public DistributedBinaryData(int id, boolean haveSynopsis, String regex) {
		this.id = id;
		this.haveSynopsis = haveSynopsis;
		this.regex = regex;
	}

	public void setSynopsis(int synopsisType, int[] synopsisSize) {
		this.synopsisType = synopsisType;
		this.synopsisSize = synopsisSize;
	}

	public void setUniformSampleRate(double uniformSampleRate) {
		this.uniformSampleRate = uniformSampleRate;
	}

	public ArrayList<Long> getTotalFileSizeInBytes() {
		ArrayList<Long> size = new ArrayList<Long>();
		size.add(totalFileSizeInBytes);

		return size;
	}

	public ParquetRow getParquetRow() {
		return this.row;
	}

	public void buildFile(FSDataOutputStream writer, List<String> lines, double MinMinLon, double MinMinLat,
			double MaxMaxLon, double MaxMaxLat) {

//		int totalFileSizeInBytes = 0;

		try {
			double[][] dataArray = null;
			if (haveSynopsis && (synopsisType == 4 || synopsisType == 5)) {
				dataArray = new double[lines.size()][4];
			} else {
				dataArray = new double[lines.size()][2];
			}
			ArrayList<double[]> dataList = new ArrayList<double[]>();
//			System.out.println("line size = " + lines.size());
			for (int i = 1; i < lines.size(); i++) {

				String line = lines.get(i);

				// process data
				String[] tmp = line.split(regex);
				double lon = Double.parseDouble(tmp[0]);
				MinMinLon = Math.min(MinMinLon, lon);
				MaxMaxLon = Math.max(MaxMaxLon, lon);

				double lat = Double.parseDouble(tmp[1]);
				MinMinLat = Math.min(MinMinLat, lat);
				MaxMaxLat = Math.max(MaxMaxLat, lat);

				if (haveSynopsis && (synopsisType == 0 || synopsisType == 1 || synopsisType == 2)) {
					double[] data = new double[2];
					data[0] = lon;
					data[1] = lat;
					dataArray[i] = data;

					ByteBuffer binaryData = ByteBuffer.allocate(Double.BYTES * 2);
					binaryData.putDouble(lon);
					binaryData.putDouble(lat);
					writer.write(binaryData.array());
				}

				if (haveSynopsis && synopsisType == 3) {

					ByteBuffer binaryData = ByteBuffer.allocate(Double.BYTES * 2);
					binaryData.putDouble(lon);
					binaryData.putDouble(lat);
					writer.write(binaryData.array());
				}

				if (haveSynopsis && (synopsisType == 4 || synopsisType == 5)) {
					double[] data = new double[4];
					// This is a class prepared for loading exp
					// The tested dataset is two-dimensional point dataset
					// Let lon + 0.35 and lat + 0.17 to make a two-dimensional polygon dataset
					data[0] = lon;
					data[2] = lon + 0.35;
					data[1] = lat;
					data[3] = lat + 0.17;
					dataArray[i] = data;

					MaxMaxLon = Math.max(MaxMaxLon, lon + 0.35);
					MaxMaxLat = Math.max(MaxMaxLat, lat + 0.17);

					ByteBuffer binaryData = ByteBuffer.allocate(Double.BYTES * 4);
					binaryData.putDouble(data[0]);
					binaryData.putDouble(data[1]);
					binaryData.putDouble(data[2]);
					binaryData.putDouble(data[3]);
					writer.write(binaryData.array());
				}

				if (haveSynopsis && (synopsisType == 6 || synopsisType == 7)) {
					double[] data = new double[2];
					data[0] = lon;
					data[1] = lat;
					dataList.add(data);

					ByteBuffer binaryData = ByteBuffer.allocate(Double.BYTES * 2);
					binaryData.putDouble(lon);
					binaryData.putDouble(lat);
					writer.write(binaryData.array());
				}

				if (synopsisType == -1) {
					double[] data = new double[2];
					data[0] = lon;
					data[1] = lat;
					ByteBuffer binaryData = ByteBuffer.allocate(Double.BYTES * 2);
					binaryData.putDouble(lon);
					binaryData.putDouble(lat);
					writer.write(binaryData.array());
				}
			}

			writer.close();

			double[] ranges = new double[4];
			ranges[0] = MinMinLon;
			ranges[1] = MaxMaxLon;
			ranges[2] = MinMinLat;
			ranges[3] = MaxMaxLat;

			// create synopsis and Parquet row

			switch (synopsisType) {
			case -1:
				// no synopsis
				this.row = new ParquetRow(id);
				row.setRange(ranges);
				break;
			case 0:
				// 1D Count histogram
				break;
			case 1:
				// 2D Count histogram
				ParquetRow2DCountHistogram histRow = new ParquetRow2DCountHistogram(id);
				SimpleSpatialHistogramOpt hist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
						MaxMaxLat, synopsisSize[0], synopsisSize[1]);
				hist = hist.loadHistByDataArray(hist, dataArray);
				histRow.setRange(synopsisSize, ranges);
				double[] shapes = new double[2];
				shapes[0] = hist.getLonUnit();
				shapes[1] = hist.getLatUnit();
				histRow.setShape(shapes);
				histRow.setCount(hist.getData());
				this.row = histRow;
				break;
			case 3:
				// 2D Wavelets
				ParquetRow2DWavelets waveletsRow = new ParquetRow2DWavelets(id);
				SimpleSpatialHistogramOpt histForWavelet = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat,
						MaxMaxLon, MaxMaxLat, synopsisSize[0], synopsisSize[1]);
				histForWavelet = histForWavelet.loadHistByDataArray(histForWavelet, dataArray);
				double[] waveletShapes = new double[2];
				waveletShapes[0] = histForWavelet.getLonUnit();
				waveletShapes[1] = histForWavelet.getLatUnit();
//				Wavelet2D wavelet = new Wavelet2D(histForWavelet);
//				String[] coeffStringArrays = wavelet.getCoeffStringArray();
				waveletsRow.setRange(ranges);
//				waveletsRow.setCoeffStringArray(coeffStringArrays);
				waveletsRow.setCount(histForWavelet.getData());
				this.row = waveletsRow;
				break;
			case 4:
				// Geometric Histogram
				ParquetRowGeometricHistogram geoHistRow = new ParquetRowGeometricHistogram(id);
				GeometricHistogramOpt geoHist = new GeometricHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon, MaxMaxLat,
						synopsisSize[0], synopsisSize[1]);
				geoHist = geoHist.loadHistByDataArray(geoHist, dataArray);
				geoHistRow.setRange(synopsisSize, ranges);
				double[] geoShapes = new double[2];
				geoShapes[0] = geoHist.getLonUnit();
				geoShapes[1] = geoHist.getLatUnit();
				geoHistRow.setShape(geoShapes);
				geoHistRow.setCorners(geoHist.getCount());
				geoHistRow.setArea(geoHist.getArea());
				geoHistRow.setHorizontal(geoHist.getRatioH());
				geoHistRow.setVertical(geoHist.getRatioV());
				this.row = geoHistRow;
				break;
			case 5:
				// Spatial Sketches
				ParquetRowSpatialSketch spatialSketchRow = new ParquetRowSpatialSketch(id);
				// spatial sketch stores counters for two dimensions
				int total = synopsisSize[0] * synopsisSize[1] / 2;
				int numOfLevel = DyadicSpatialSketch.computeH(total);
				DyadicSpatialSketch sketchX = new DyadicSpatialSketch(MinMinLon, MaxMaxLon, numOfLevel);
				DyadicSpatialSketch sketchY = new DyadicSpatialSketch(MinMinLat, MaxMaxLat, numOfLevel);
				sketchX.loadHistByDataArray(sketchX, dataArray, 0, 2);
				sketchY.loadHistByDataArray(sketchY, dataArray, 1, 3);
				spatialSketchRow.setIntervalCountersSketchX(sketchX.getIntervalsCoints());
				spatialSketchRow.setEndPointCountersSketchX(sketchX.getPointCounters());
				spatialSketchRow.setIntervalCountersSketchY(sketchY.getIntervalsCoints());
				spatialSketchRow.setEndPointCountersSketchY(sketchY.getPointCounters());
				this.row = spatialSketchRow;
				break;
			case 6:
				// Uniform Samples
				ParquetRowUniformSamples<double[]> uniformSamplesRow = new ParquetRowUniformSamples<>(id);
				UniformSamples<double[]> uniformSamples = new UniformSamples<double[]>(dataList.size(),
						uniformSampleRate);
				uniformSamplesRow.setSampleRatio(uniformSampleRate);
				uniformSamplesRow.setSampleData(uniformSamples.Sample(dataList));
//				System.out.println("sample size = " + uniformSamples.getSampleData().size());
				uniformSamplesRow.setRange(ranges);
				this.row = uniformSamplesRow;
				break;
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void buildFileWithoutData(List<String> lines, double MinMinLon, double MinMinLat, double MaxMaxLon,
			double MaxMaxLat) {

//		int totalFileSizeInBytes = 0;

		double[][] dataArray = null;
		if (haveSynopsis && (synopsisType == 4 || synopsisType == 5)) {
			dataArray = new double[lines.size()][4];
		} else {
			dataArray = new double[lines.size()][2];
		}
		ArrayList<double[]> dataList = new ArrayList<double[]>();

		for (int i = 1; i < lines.size(); i++) {

			String line = lines.get(i);

//				// write data
//				byte[] binaryData = line.getBytes();
//				writer.write(binaryData);

			// process data
			String[] tmp = line.split(regex);
			double lon = Double.parseDouble(tmp[0]);
//			MinMinLon = Math.min(MinMinLon, lon);
//			MaxMaxLon = Math.max(MaxMaxLon, lon);

			double lat = Double.parseDouble(tmp[1]);
//			MinMinLat = Math.min(MinMinLat, lat);
//			MaxMaxLat = Math.max(MaxMaxLat, lat);

			if (haveSynopsis && (synopsisType == 0 || synopsisType == 1 || synopsisType == 2 || synopsisType == 3)) {
				double[] data = new double[2];
				data[0] = lon;
				data[1] = lat;
				dataArray[i] = data;
			}

			if (haveSynopsis && (synopsisType == 4 || synopsisType == 5)) {
				double[] data = new double[4];
				// This is a class prepared for loading exp
				// The tested dataset is two-dimensional point dataset
				// Let lon + 0.35 and lat + 0.17 to make a two-dimensional polygon dataset
				data[0] = lon;
				data[2] = lon + 0.35;
				data[1] = lat;
				data[3] = lat + 0.17;
				dataArray[i] = data;
			}

			if (haveSynopsis && (synopsisType == 6 || synopsisType == 7)) {
				double[] data = new double[2];
				data[0] = lon;
				data[1] = lat;
				dataList.add(data);
			}

		}

//			writer.close();

		double[] ranges = new double[4];
		ranges[0] = MinMinLon;
		ranges[1] = MaxMaxLon;
		ranges[2] = MinMinLat;
		ranges[3] = MaxMaxLat;

		// create synopsis and Parquet row

		switch (synopsisType) {
		case -1:
			// no synopsis
			this.row = new ParquetRow(id);
			row.setRange(ranges);
			break;
		case 0:
			// 1D Count histogram
			break;
		case 1:
			// 2D Count histogram
			ParquetRow2DCountHistogram histRow = new ParquetRow2DCountHistogram(id);
//				setRange(int[] dimensionSizes, double[] range) {
			SimpleSpatialHistogramOpt hist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon, MaxMaxLat,
					synopsisSize[0], synopsisSize[1]);
			hist = hist.loadHistByDataArray(hist, dataArray);
			histRow.setRange(synopsisSize, ranges);
			double[] shapes = new double[2];
			shapes[0] = hist.getLonUnit();
			shapes[1] = hist.getLatUnit();
			histRow.setShape(shapes);
			histRow.setCount(hist.getData());
			this.row = histRow;
			break;
		case 3:
			// 2D Wavelets
			ParquetRow2DWavelets waveletsRow = new ParquetRow2DWavelets(id);
			SimpleSpatialHistogramOpt histForWavelet = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
					MaxMaxLat, synopsisSize[0], synopsisSize[1]);
			histForWavelet = histForWavelet.loadHistByDataArray(histForWavelet, dataArray);
			double[] waveletShapes = new double[2];
			waveletShapes[0] = histForWavelet.getLonUnit();
			waveletShapes[1] = histForWavelet.getLatUnit();
			Wavelet2D wavelet = new Wavelet2D(histForWavelet);
//			String[] coeffStringArrays = wavelet.getCoeffStringArray();
			waveletsRow.setRange(ranges);
//			waveletsRow.setCoeffStringArray(coeffStringArrays);
//			waveletsRow.setCoeffStringArray(coeffStringArrays);
			waveletsRow.setCount(histForWavelet.getData());
			this.row = waveletsRow;
			this.row = waveletsRow;
			break;
		case 4:
			// Geometric Histogram
			ParquetRowGeometricHistogram geoHistRow = new ParquetRowGeometricHistogram(id);
			GeometricHistogramOpt geoHist = new GeometricHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon, MaxMaxLat,
					synopsisSize[0], synopsisSize[1]);
//			System.out.println("GG dataArray size = " + dataArray.length + ", di = " + dataArray[0].length);
			geoHist = geoHist.loadHistByDataArray(geoHist, dataArray);
			geoHistRow.setRange(synopsisSize, ranges);
			double[] geoShapes = new double[2];
			geoShapes[0] = geoHist.getLonUnit();
			geoShapes[1] = geoHist.getLatUnit();
			geoHistRow.setShape(geoShapes);
			geoHistRow.setCorners(geoHist.getCount());
			geoHistRow.setArea(geoHist.getArea());
			geoHistRow.setHorizontal(geoHist.getRatioH());
			geoHistRow.setVertical(geoHist.getRatioV());
			this.row = geoHistRow;
			break;
		case 5:
			// Spatial Sketches
			ParquetRowSpatialSketch spatialSketchRow = new ParquetRowSpatialSketch(id);
			int total = synopsisSize[0] * synopsisSize[1] / 2;
			int numOfLevel = DyadicSpatialSketch.computeH(total);
			DyadicSpatialSketch sketchX = new DyadicSpatialSketch(MinMinLon, MaxMaxLon, numOfLevel);
			DyadicSpatialSketch sketchY = new DyadicSpatialSketch(MinMinLat, MaxMaxLat, numOfLevel);
			sketchX.loadHistByDataArray(sketchX, dataArray, 0, 2);
			sketchY.loadHistByDataArray(sketchY, dataArray, 1, 3);
			spatialSketchRow.setIntervalCountersSketchX(sketchX.getIntervalsCoints());
			spatialSketchRow.setEndPointCountersSketchX(sketchX.getPointCounters());
			spatialSketchRow.setIntervalCountersSketchY(sketchY.getIntervalsCoints());
			spatialSketchRow.setEndPointCountersSketchY(sketchY.getPointCounters());
			this.row = spatialSketchRow;
			break;
		case 6:
			// Uniform Samples
			ParquetRowUniformSamples<double[]> uniformSamplesRow = new ParquetRowUniformSamples<>(id);
			UniformSamples<double[]> uniformSamples = new UniformSamples<>(dataList.size(), uniformSampleRate);
//			uniformSamples.loadSampleByDataList(uniformSamples, dataList);
			uniformSamplesRow.setSampleRatio(uniformSampleRate);
			uniformSamplesRow.setSampleData(uniformSamples.Sample(dataList));
			uniformSamplesRow.setRange(ranges);
			this.row = uniformSamplesRow;
			break;
		}

	}
}
