package samples;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StratifiedSamples implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7121636984968340168L;
	
	private int numPar = -1;
	private double sampleRate = -1.0;
	private int[] parCounts;
	
	public StratifiedSamples(int numPar, double sampleRate) {
		this.numPar = numPar;
		this.sampleRate = sampleRate;
		this.parCounts = new int[numPar];
	}
	
	public List<List<Double>> Sample(double min, double max, ArrayList<Double> items) {
		
		// step 1: partition items
		ArrayList<ArrayList<Double>> partitionedItem = new ArrayList<ArrayList<Double>>();
		for (int i=0; i<numPar; i++) {
			partitionedItem.add(new ArrayList<Double>());
		}
		double unit = (max - min) / numPar;
		for (int i=0; i<items.size(); i++) {
			int pId = (int) ((items.get(i) - min) / unit);
			partitionedItem.get(pId).add(items.get(i));
			parCounts[pId]++;
		}
		
		// step 2: call uniform samples
		List<List<Double>> samples = new ArrayList<List<Double>>();
		for (int pId=0; pId<numPar; pId++) {
			int sampleLen = parCounts[pId];
			
			UniformSamples<Double> samplesModel = new UniformSamples<>(sampleLen, this.sampleRate);
			samplesModel.Sample(partitionedItem.get(pId));
			samples.add(samplesModel.getSampleData());
		}
		
		return samples;
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ArrayList<Double> items = new ArrayList<Double>();
		for (int i=0; i<100; i++) {
			items.add((double)i);
		}
		StratifiedSamples samplesModel = new StratifiedSamples(5, 0.5);
		List<List<Double>> samples = samplesModel.Sample(0.0, 100.0, items);
		System.out.println(samples);

	}

}
