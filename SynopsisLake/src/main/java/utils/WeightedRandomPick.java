package utils;

import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

public class WeightedRandomPick<E> {
	private final NavigableMap<Double, E> map = new TreeMap<Double, E>();
	private final Random random;
	private double total = 0;

	public WeightedRandomPick() {
		this(new Random());
	}

	public WeightedRandomPick(Random random) {
			this.random = random;
		}

	public WeightedRandomPick<E> add(double weight, E result) {
		if (weight <= 0)
			return this;
		total += weight;
		map.put(total, result);
		return this;
	}

	public E next() {
		double value = random.nextDouble() * total;
		return map.higherEntry(value).getValue();
	}

	public static void main(String[] args) {

		WeightedRandomPick<String> rc = new WeightedRandomPick<String>()
				.add(50, "dog").add(35, "cat").add(15, "horse")
				.add(0, "lizard");

		for (int i = 0; i < 10; i++) {
			System.out.println(rc.next());
		}
	}

}
