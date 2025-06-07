package baseline;

public class NearOptimalPos {
	
	// the indexes of coordinate, i_{j-1}, i_j, j_{j+1}
	public int iPre = -1;
	public int i = -1;
	public int iNext = -1;
	
//	// the frequencies
//	public double r = 0; // partial sum \sum {y_u}
//	public double t = 0; // sum of partial sum sqr \sum {y_u^2}
	public NearOptimalPos(int iPre, int i, int iNext) {
		this.iPre = iPre;
		this.i = i;
		this.iNext = iNext;
	}
	
//	public NearOptimalInterval(int iPre, int iNext) {
//		this.iPre = iPre;
//		this.iNext = iNext;
//	}
	
	@Override
	public String toString() {
		return iPre + "-" + i + "-" + iNext;
	}
}
