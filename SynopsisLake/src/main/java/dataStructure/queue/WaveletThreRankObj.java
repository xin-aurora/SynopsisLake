package dataStructure.queue;

public class WaveletThreRankObj {
	public double v;
	public int weight; // number of i_j=0
	public int[] idxs;
	public WaveletThreRankObj(double v, int weight, int[] idxs) {
		this.v = v;
		this.weight = weight;
		this.idxs = idxs;
	}
}
