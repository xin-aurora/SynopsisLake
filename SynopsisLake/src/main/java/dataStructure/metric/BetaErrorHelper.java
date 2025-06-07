package dataStructure.metric;

import java.util.HashMap;

public class BetaErrorHelper implements Comparable<BetaErrorHelper> {
	public int bId;
	public double errorDiff;
	// left info
	public int leftBId;
	// merge info
	public double errSrcToTar;
	public double errTarToSrc;
	public double errAbsTarToSrc;
	public double densityDiffs;
	public HashMap<Integer, Double> srcIdToAbsErr = new HashMap<Integer, Double>();
	public BetaErrorHelper(int bId) {
		this.bId = bId;
		errorDiff = 0;
	}
	
	@Override
	public int hashCode() {
		return this.bId;
	}
	
	@Override
	public int compareTo(BetaErrorHelper o) {
		// TODO Auto-generated method stub
		return Double.compare(errorDiff, o.errorDiff);
	}
	
	@Override
	public String toString() {
		return "[" + bId + "]: " + errorDiff;
	}
}
