package dataStructure.parquet;

import java.io.Serializable;

public class ParquetRowSpatialSketch extends ParquetRow implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -793911332666817904L;

	public ParquetRowSpatialSketch(int id) {
		super(id);
		this.synopsisType = 5;
	}

}
