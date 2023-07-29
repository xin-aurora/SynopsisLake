package dataStructure.parquet;

import java.io.Serializable;

public class ParquetRowStratifiedSamples<T> extends ParquetRow implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1871656138855526258L;
	
	private double sampleRatio;
	private T[] partitions;
	private T[][] samples;

	public ParquetRowStratifiedSamples(int id) {
		super(id);
		this.synopsisType = 7;
	}

}
