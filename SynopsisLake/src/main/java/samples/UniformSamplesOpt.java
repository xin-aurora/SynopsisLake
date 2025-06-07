package samples;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

public class UniformSamplesOpt<T> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4492494256023436055L;

	private int totalNumItems = -1;
	private double sampleRate = -1.0;

	public UniformSamplesOpt(int totalNumItems, double sampleRate) {
		this.totalNumItems = totalNumItems;
		this.sampleRate = sampleRate;
	}

	public List<T> Sample(ArrayList<T> items) {

		int sampleLen = (int) Math.ceil(totalNumItems * sampleRate);

		List<T> samples = new ArrayList<T>(sampleLen);

		int[] randomIdxs = IntStream.generate(() -> new Random().nextInt(totalNumItems)).distinct()
				.limit(sampleLen).toArray();
		for (int i = 0; i < sampleLen; i++) {
			samples.add(items.get(randomIdxs[i]));
		}
		return samples;
	}

	public int getTotalNumItems() {
		return this.totalNumItems;
	}

	public double getSampleRate() {
		return this.sampleRate;
	}

}
