package accumulator;

import java.util.ArrayList;

import org.apache.spark.util.AccumulatorV2;

public class BinaryFileSizeAccumulator<T> extends AccumulatorV2<T, ArrayList<T>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8948505485436592019L;
	private ArrayList<T> sizeList = new ArrayList<T>();


	@Override
	public AccumulatorV2<T, ArrayList<T>> copy() {
		// TODO Auto-generated method stub
		BinaryFileSizeAccumulator<T> newItem = new BinaryFileSizeAccumulator();
		newItem.merge(this);
		return newItem;
	}

	@Override
	public boolean isZero() {
		// TODO Auto-generated method stub
		return this.sizeList.isEmpty();
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		this.sizeList = new ArrayList<T>();
	}

	@Override
	public ArrayList<T> value() {
		// TODO Auto-generated method stub
		return this.sizeList;
	}

	@Override
	public void add(T v) {
		// TODO Auto-generated method stub
		this.sizeList.add(v);
	}

	@Override
	public void merge(AccumulatorV2<T, ArrayList<T>> other) {
		// TODO Auto-generated method stub
		for (T item: other.value()) {
			this.sizeList.add(item);
		}
	}
}
