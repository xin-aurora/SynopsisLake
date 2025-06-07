package dataStructure;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The implementation is extended based on
 * https://github.com/javatuples/javatuples,
 * https://github.com/javatuples/javatuples/blob/master/src/main/java/org/javatuples/Tuple.java
 * 
 * @author xin_aurora
 *
 * @param <A>
 * @param <B>
 */
public abstract class Tuple implements Serializable{

	private static final long serialVersionUID = -8449616505717727679L;
	
	private final Object[] valueArray;
	private final List<Object> valueList;

	protected Tuple(final Object... values) {
		super();
		this.valueArray = values;
		this.valueList = Arrays.asList(values);
	}
	
	public final List<Object> toList() {
        return Collections.unmodifiableList(new ArrayList<Object>(this.valueList));
    }
    
    public final Object[] toArray() {
        return this.valueArray.clone();
    }

	// return size of the tuple
	public abstract int getSize();

	@Override
	public final String toString() {
		return this.valueList.toString();
	}

	@Override
	public final int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.valueList == null) ? 0 : this.valueList.hashCode());
		return result;
	}

	@Override
	public final boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final Tuple other = (Tuple) obj;
		return this.valueList.equals(other.valueList);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public int compareTo(final Tuple o) {

		final int tLen = this.valueArray.length;
		final Object[] oValues = o.valueArray;
		final int oLen = oValues.length;

		for (int i = 0; i < tLen && i < oLen; i++) {

			final Comparable tElement = (Comparable) this.valueArray[i];
			final Comparable oElement = (Comparable) oValues[i];

			final int comparison = tElement.compareTo(oElement);
			if (comparison != 0) {
				return comparison;
			}

		}

		return (Integer.valueOf(tLen)).compareTo(Integer.valueOf(oLen));

	}
}
