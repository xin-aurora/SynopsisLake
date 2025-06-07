package samples;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class UniformSamples<T> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4492494256023436055L;

	private int totalNumItems = -1;
	private double sampleRate = -1.0;
//	List<T> samplesData;

	public UniformSamples(int totalNumItems, double sampleRate) {
		this.totalNumItems = totalNumItems;
		this.sampleRate = sampleRate;
	}

	public List<T> Sample(ArrayList<T> items) {

		int sampleLen = (int) Math.ceil(totalNumItems * sampleRate);
//		System.out.println("sampleLen = " + sampleLen);

		Collections.shuffle(items);

		return new ArrayList<T>(items.subList(0, sampleLen));
	}


//	public void setSamples(List<T> samplesData) {
//		this.samplesData = samplesData;
//	}

	public int getTotalNumItems() {
		return this.totalNumItems;
	}

	public double getSampleRate() {
		return this.sampleRate;
	}

//	public List<T> getSampleData() {
//		return this.samplesData;
//	}

//	public UniformSamples<T> loadSampleByDataList(UniformSamples<T> samples, ArrayList<T> dataList) {
//
//		int sampleLen = (int) (samples.getSampleRate() * samples.getTotalNumItems());
//		Collections.shuffle(dataList);
//
//		List<T> samplesData = new ArrayList<T>(dataList.subList(0, sampleLen));
//		samples.setSamples(samplesData);
//
//		return samples;
//
//	}

//	public static void main(String[] args) {
//		ArrayList<Integer> items = new ArrayList<Integer>();
//		for (int i=0; i<20; i++) {
//			items.add(i);
//		}
//		UniformSamples<Integer> samplesModel = new UniformSamples<Integer>(20, 0.45);
//		samplesModel.Sample(items);
//		List<Integer> samples = samplesModel.getSampleData();
//		System.out.println(samples);
//		System.out.println(samples.size());
//	}

}
