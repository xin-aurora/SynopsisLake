package operator;

import java.util.ArrayList;

public class BetaMetric2DPartitioning {

	int totalBudget = 0;
	int numDim = 0;
	int[] budgets;

	public BetaMetric2DPartitioning(int totalBudget, int numDim) {
		this.totalBudget = totalBudget;
		this.numDim = numDim;
		this.budgets = new int[numDim];
	}

	private void equalSplitBudget() {
		int budget = (int) Math.pow(totalBudget, numDim);
		for (int i = 0; i < numDim; i++) {
			budgets[i] = budget;
		}
	}

	private void ratioSplitBudget(int[] numSrcBuckets) {
		double totalSrcBuckets = 0;
		for (int i = 0; i < numDim; i++) {
			totalSrcBuckets += numSrcBuckets[i];
		}

		for (int i = 0; i < numDim; i++) {
			budgets[i] = (int) (totalBudget * numSrcBuckets[i] / totalSrcBuckets);
		}
	}
	
	public void reshaping(ArrayList<double[]> lons, ArrayList<double[]> lats) {
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
