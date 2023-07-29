package dataStructure.parquet;

import java.io.Serializable;
import java.util.List;

public class ParquetRowUniformSamples<T> extends ParquetRow implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2133371130660996824L;
	private double sampleRatio;
	private double[][] sampleData;

	public ParquetRowUniformSamples(int id) {
		super(id);
		this.synopsisType = 6;
	}
	
	public void setSampleRatio(double sampleRatio) {
		this.sampleRatio = sampleRatio;
	}
	
	public void setSampleData(List<double[]> samples) {
		this.sampleData = new double[samples.size()][samples.get(0).length];
		samples.toArray(sampleData);
	}
	
	public double getSampleRatio() {
		return sampleRatio;
	}

	public double[][] getSampleData() {
		return sampleData;
	}

}
