package dataStructure;

/**
 * The implementation is extended based on https://github.com/javatuples/javatuples,
 * https://github.com/javatuples/javatuples/blob/master/src/main/java/org/javatuples/Pair.java
 * @author xin_aurora
 *
 * @param <A>
 * @param <B>
 */
public class Tuple2<A, B> extends Tuple {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6190930327482409638L;

	private static final int SIZE = 2;

	private final A val0;
	private final B val1;
	
	public Tuple2(
			final A value0, 
			final B value1) {
		super(value0, value1);
        this.val0 = value0;
        this.val1 = value1;
    }
	
	public A getValue0() {
        return this.val0;
    }


    public B getValue1() {
        return this.val1;
    }

    @Override
    public int getSize() {
        return SIZE;
    }

}
