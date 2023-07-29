package operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;

import dataStructure.metric.JMBoundary1D;
import dataStructure.metric.JMSrcInterval;
import dataStructure.queue.MyPriorityQueue;

public class JaccardMetricQueue1D {

	private double quality = -1;
	private int targetQueueSize = -1;
//	private int totalSrcIntervalSize = -1;
	private double min = -1;
	private double max = -1;

//	private HashMap<Integer, JMSrcInterval> idToInterval = new HashMap<Integer, JMSrcInterval>();
	private JMSrcInterval[] idToInterval;
//	private HashMap<Integer, JMBoundary1D> idToBoundary = new HashMap<Integer, JMBoundary1D>();

	private ArrayList<HashSet<Integer>> overlapSrInfos = new ArrayList<HashSet<Integer>>();

//	private int nextBoundaryId = 0;
	private int nextIntervalId = 0;

	public JaccardMetricQueue1D(double quality, int targetSize, int totalSrcIntervalSize, double min, double max) {
		this.quality = quality;
		// input targetSize: number target interval
		// targetSize + 1 = boundary size
		// targetSize + 1 - 2 = queue size
		this.targetQueueSize = targetSize - 1;
//		this.totalSrcIntervalSize = totalSrcIntervalSize;
		idToInterval = new JMSrcInterval[totalSrcIntervalSize];
		this.min = min;
		this.max = max;

	}

	public MyPriorityQueue<JMBoundary1D> reshapping(ArrayList<ArrayList<Double>> sources, double[] weights) {
//		ArrayList<JMBoundary1D> opt = optimalSolution(sources, weights);
		
		long startTime = System.nanoTime();
		ArrayList<JMBoundary1D> opt = optimalSolution(sources, weights);
		long endTime = System.nanoTime();
		System.out.println("generate opt solution time = " + (endTime - startTime) * 1E-9 + " s.");

		return reduceNumInterval(opt, sources, weights);

	}

	public ArrayList<Double> getResolution(MyPriorityQueue<JMBoundary1D> queue) {

//		System.out.println("queue size = " + queue.size());
		ArrayList<Double> opt = new ArrayList<Double>();
		while (!queue.isEmpty()) {
//			System.out.println("queue size = " + queue.size());
			double v = queue.poll().getVal();
//			System.out.println(v);
			opt.add(v);
		}
		opt.add(min);
		opt.add(max);

		Collections.sort(opt);
//		System.out.println(opt.size() + "!!! opt = " + opt);

		return opt;
	}
	
	public double[] getResolutionArray(MyPriorityQueue<JMBoundary1D> queue) {

//		System.out.println("queue size = " + queue.size());
		ArrayList<Double> opt = new ArrayList<Double>();
		while (!queue.isEmpty()) {
//			System.out.println("queue size = " + queue.size());
			double v = queue.poll().getVal();
//			System.out.println(v);
			opt.add(v);
		}
		opt.add(min);
		opt.add(max);

		Collections.sort(opt);
//		System.out.println(opt.size() + "!!! opt = " + opt);
		
		double[] optArray = new double[opt.size()];
		for (int i=0; i<optArray.length; i++) {
			optArray[i] = opt.get(i);
		}

		return optArray;
	}

	private MyPriorityQueue<JMBoundary1D> reduceNumInterval(ArrayList<JMBoundary1D> opt,
			ArrayList<ArrayList<Double>> sources, double[] weights) {
		
		System.out.println("num of initial opt boundary = " + opt.size());
		long startTime = System.nanoTime();
		
		MyPriorityQueue<JMBoundary1D> initialQueue = initialTargetQueue(opt);
		
		long endTime = System.nanoTime();
		System.out.println("initial target queue time = " + (endTime - startTime) * 1E-9 + " s.");
		
//		System.out.println(initialQueue.getRest());

		startTime = System.nanoTime();
		while (initialQueue.size() > targetQueueSize) {
//			System.out.println("initialQueue size = " + initialQueue.size() + ", targetQueueSize = " + targetQueueSize);
			JMBoundary1D boundary = initialQueue.poll();
			int bId = boundary.getId();
//			System.out.println("pop boundary = " + boundary);
			// update its left boundary and right boundary
			int leftId = boundary.getLBId();
			int rightBId = boundary.getRBId();
			JMBoundary1D leftB = opt.get(leftId);
			JMBoundary1D rightB = opt.get(rightBId);
//			System.out.println("pop boundary = " + boundary + ", left = " + leftB + ", right = " + rightB);
			// merge left and right overlap src intervals
			overlapSrInfos.set(bId, boundary.overlaps);
			overlapSrInfos.set(rightBId, boundary.overlaps);

			// update left boundary
			leftB.setRightB(rightB.getVal(), rightBId);
			// true: down, false: up
			if (leftId != 0) {
				boolean isBigger = leftB.computeScore(idToInterval, overlapSrInfos.get(leftId), boundary.overlaps,
						leftB.getLeftScore(), boundary.getMergeScore());
				initialQueue.updatePriority(leftB, isBigger);
			}

			// update right boundary
			rightB.setLeftB(leftB.getVal(), leftId);
			if (rightBId != (opt.size() - 1)) {
				int rightRightId = rightB.getRBId();
				boolean isBiggerR = rightB.computeScore(idToInterval, boundary.overlaps, overlapSrInfos.get(rightRightId),
						boundary.getMergeScore(), rightB.getRightScore());
				initialQueue.updatePriority(rightB, isBiggerR);
			}
		}
		
		endTime = System.nanoTime();
		System.out.println("drop boundary time = " + (endTime - startTime) * 1E-9 + " s.");

		return initialQueue;
	}

	private MyPriorityQueue<JMBoundary1D> initialTargetQueue(ArrayList<JMBoundary1D> opt) {

		MyPriorityQueue<JMBoundary1D> initialQueue = new MyPriorityQueue<JMBoundary1D>();
		opt.get(0).setRightB(opt.get(1).getVal(), 1);
		opt.get(0).computeScore(idToInterval, overlapSrInfos.get(0), overlapSrInfos.get(1), 0, -1);
		for (int t = 1; t < opt.size() - 1; t++) {
			opt.get(t).setLeftB(opt.get(t - 1).getVal(), t - 1);
			opt.get(t).setRightB(opt.get(t + 1).getVal(), t + 1);
			opt.get(t).computeScore(idToInterval, overlapSrInfos.get(t), overlapSrInfos.get(t + 1),
					opt.get(t - 1).getRightScore(), -1);
			initialQueue.add(opt.get(t));
		}
		int lastIdx = opt.size() - 1;
		opt.get(lastIdx).setLeftB(opt.get(lastIdx - 1).getVal(), lastIdx - 1);
//		opt.get(lastIdx).computeScore(idToInterval, overlapSrInfos.get(lastIdx), new HashSet<Integer>(), -1, 0);

		return initialQueue;
	}

	private ArrayList<JMBoundary1D> optimalSolution(ArrayList<ArrayList<Double>> sources, double[] weights) {
		ArrayList<JMBoundary1D> opt = new ArrayList<JMBoundary1D>();

		int numSrc = sources.size();
//		System.out.println("numSrc = " + numSrc);

		// the left boundary of each src interval
		double[] leftBoundaries = new double[numSrc];

		// the right boundary of each src interval
		PriorityQueue<OptSrcEndBoundary> rightBoundaries = new PriorityQueue<>(new Comparator<OptSrcEndBoundary>() {

			@Override
			public int compare(OptSrcEndBoundary o1, OptSrcEndBoundary o2) {
				// TODO Auto-generated method stub
				return Double.compare(o1.val, o2.val);
			}
		});

		// number srcBoundary in each src histogram
		int[] numSrcBoundaryInOneSrc = new int[numSrc];
		
		int numBoundary = 0;
		int totalNumBoundary = idToInterval.length + numSrc;
		// the idx of boundary of each src interval
		int[] idxs = new int[numSrc];

		// initialize the rightBoundary queue
		for (int srcId = 0; srcId < numSrc; srcId++) {
			idxs[srcId] = 1;
			OptSrcEndBoundary srcBoundary = new OptSrcEndBoundary(srcId, sources.get(srcId).get(0), weights[srcId]);
			rightBoundaries.add(srcBoundary);
			numSrcBoundaryInOneSrc[srcId] = sources.get(srcId).size();
		}
		
//		System.out.println(Arrays.toString(numSrcBoundaryInOneSrc));

		double preOptBoundaryVal = -1;
		int optBId = 0; // opt boundary id
		int srcIId = 0; // src interval id
		while (numBoundary < totalNumBoundary) {
			OptSrcEndBoundary optBoundaryCandidate = rightBoundaries.poll();
//			System.out.println("optBId = " + optBoundaryCandidate);
			// if its a new boundary val, create it as a new opt boundary
			if (optBoundaryCandidate.val != preOptBoundaryVal) {
				JMBoundary1D optB = new JMBoundary1D(optBId, optBoundaryCandidate.val);
				// add it to opt list
				opt.add(optB);
				// create left overlapping set
				HashSet<Integer> leftOverlap = new HashSet<Integer>();
				overlapSrInfos.add(leftOverlap);
				optBId++;
				preOptBoundaryVal = optBoundaryCandidate.val;
//				System.out.println("insert " + optB);
			}
			// put the next src boundary into the right queue
			int nextId = idxs[optBoundaryCandidate.srcId];
			if (nextId < numSrcBoundaryInOneSrc[optBoundaryCandidate.srcId]) {
				OptSrcEndBoundary srcBoundary = new OptSrcEndBoundary(optBoundaryCandidate.srcId,
						sources.get(optBoundaryCandidate.srcId).get(nextId), weights[optBoundaryCandidate.srcId]);
				rightBoundaries.add(srcBoundary);
				idxs[optBoundaryCandidate.srcId] = nextId + 1;
			}
			// create a src interval
			if (nextId > 1) {
				JMSrcInterval src = new JMSrcInterval(srcIId, weights[optBoundaryCandidate.srcId],
						leftBoundaries[optBoundaryCandidate.srcId], optBoundaryCandidate.val);
//				System.out.println("create " + src);
				idToInterval[srcIId] = src;
				int overlapBId = optBId - 1;
				double preBoundaryVal = opt.get(overlapBId).getVal();
				double srcMin = src.min;
				while ( srcMin < preBoundaryVal && overlapBId > 0) {
					// add it to the left overlapping interval of current opt boundary
					overlapSrInfos.get(overlapBId).add(srcIId);
//					System.out.println("overlap set for " + (overlapBId) + " is: " + overlapSrInfos.get(overlapBId));
					overlapBId--;
					preBoundaryVal = opt.get(overlapBId).getVal();
				}
				srcIId++;
			}
			// put current right boundary to the left boundary array
			leftBoundaries[optBoundaryCandidate.srcId] = optBoundaryCandidate.val;
			
			numBoundary++;
//			System.out.println(Arrays.toString(leftBoundaries));
		}
		
//		System.out.println(opt);
//		System.out.println(Arrays.toString(idToInterval));
//		System.out
//				.println("overlapSrcIntervals size = " + overlapSrInfos.size() + ", opt size = " + opt.size());
//		for (int i = 0; i < overlapSrInfos.size(); i++) {
//			System.out.println(i + ": " + opt.get(i) + ", overlap = " + overlapSrInfos.get(i));
//		}

		return opt;
	}

}

class OptSrcEndBoundary {
	public int srcId;
	public double val;
	public double weight;

	public OptSrcEndBoundary(int srcId, double val, double weight) {
		this.srcId = srcId;
		this.val = val;
		this.weight = weight;
	}
	
	@Override
	public String toString() {
		return srcId + ": " + val + ", w=" + weight;
	}
}
