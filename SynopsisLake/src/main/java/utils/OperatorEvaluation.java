package utils;

import java.util.List;

public class OperatorEvaluation {

	public static double[] KMeans2DEvaluation(double[][] aggCenters, double[][] aggbsCenters,
			double[][] randomDropCenters, double[][] nearOptCenters, double[][] uniformCenters,
			List<double[]> allDataArray) {

		double[] totalDis = new double[5];
		double[] dis = KMeans2DEvaluation(allDataArray, aggCenters, aggbsCenters, randomDropCenters, nearOptCenters,
				uniformCenters);
		totalDis[0] += dis[0];
		totalDis[1] += dis[1];
		totalDis[2] += dis[2];
		totalDis[3] += dis[3];
		totalDis[4] += dis[4];

		return totalDis;
	}

	public static double[] KMeans2DEvaluation(List<double[]> allDataArray, double[][] aggCenters,
			double[][] bsAggCenters, double[][] randomDropCenters, double[][] nearOptCenters,
			double[][] uniformCenters) {

		double totalDisAgg = 0;
		double totalDistBSAgg = 0;
		double totalRandom = 0;
		double totalNearOpt = 0;
		double totalUniform = 0;

		for (int i = 0; i < allDataArray.size(); i++) {
			double disUniform = Double.MAX_VALUE;
			double disBSAgg = Double.MAX_VALUE;
			double disAgg = Double.MAX_VALUE;
			double disRandom = Double.MAX_VALUE;
			double disNearOpt = Double.MAX_VALUE;

			double[] data = allDataArray.get(i);

			for (int cId = 0; cId < aggCenters.length; cId++) {

				double[] aggCenter = aggCenters[cId];
				double[] bsAggCenter = bsAggCenters[cId];
				double[] randomCenter = randomDropCenters[cId];
				double[] nearOptCenter = nearOptCenters[cId];
				double[] uniformCenter = uniformCenters[cId];
				double dUniform = 0;
				double dBSAgg = 0;
				double dAgg = 0;
				double dRandom = 0;
				double dNearOpt = 0;

				for (int d = 0; d < 2; d++) {
					dUniform += Math.pow((data[d] - uniformCenter[d]), 2);
					dBSAgg += Math.pow((data[d] - bsAggCenter[d]), 2);
					dAgg += Math.pow((data[d] - aggCenter[d]), 2);
					dRandom += Math.pow((data[d] - randomCenter[d]), 2);
					dNearOpt += Math.pow((data[d] - nearOptCenter[d]), 2);
				}

				disUniform = Math.min(disUniform, dUniform);
				disBSAgg = Math.min(disBSAgg, dBSAgg);
				disAgg = Math.min(disAgg, dAgg);
				disRandom = Math.min(disRandom, dRandom);
				disNearOpt = Math.min(disNearOpt, dNearOpt);
			}
			totalUniform += disUniform;
			totalDistBSAgg += disBSAgg;
			totalDisAgg += disAgg;
			totalRandom += disRandom;
			totalNearOpt += disNearOpt;
		}
		totalUniform = totalUniform / allDataArray.size();
		totalDistBSAgg = totalDistBSAgg / allDataArray.size();
		totalDisAgg = totalDisAgg / allDataArray.size();
		totalRandom = totalRandom / allDataArray.size();
		totalNearOpt = totalNearOpt / allDataArray.size();

		double[] error = new double[5];
		error[0] = totalDisAgg;
		error[1] = totalDistBSAgg;
		error[2] = totalRandom;
		error[3] = totalNearOpt;
		error[4] = totalUniform;

		return error;
	}

	public static double[] KMeans2DWeightEvaluation(List<double[]> allDataArray, double[][] aggCenters,
			double[][] bsAggCenters) {

		double totalDisAgg = 0;
		double totalDistBSAgg = 0;
		double totalRandom = 0;
		double totalNearOpt = 0;
		double totalUniform = 0;

		for (int i = 0; i < allDataArray.size(); i++) {

			double disBSAgg = Double.MAX_VALUE;
			double disAgg = Double.MAX_VALUE;

			double[] data = allDataArray.get(i);

			for (int cId = 0; cId < aggCenters.length; cId++) {

				double[] aggCenter = aggCenters[cId];
				double[] bsAggCenter = bsAggCenters[cId];
	
				double dBSAgg = 0;
				double dAgg = 0;


				for (int d = 0; d < 2; d++) {

					dBSAgg += Math.pow((data[d] - bsAggCenter[d]), 2);
					dAgg += Math.pow((data[d] - aggCenter[d]), 2);

				}

				disBSAgg = Math.min(disBSAgg, dBSAgg);
				disAgg = Math.min(disAgg, dAgg);

			}

			totalDistBSAgg += disBSAgg;
			totalDisAgg += disAgg;

		}

		totalDistBSAgg = totalDistBSAgg / allDataArray.size();
		totalDisAgg = totalDisAgg / allDataArray.size();

		double[] error = new double[5];
		error[0] = totalDisAgg;
		error[1] = totalDistBSAgg;
		error[2] = totalRandom;
		error[3] = totalNearOpt;
		error[4] = totalUniform;

		return error;
	}
}
