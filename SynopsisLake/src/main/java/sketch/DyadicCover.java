package sketch;

import java.util.ArrayList;
import java.util.List;

public class DyadicCover {

    public static List<double[]> computeDyadicCover(double[] X) {
        double minVal = Double.MAX_VALUE;
        double maxVal = Double.MIN_VALUE;

        // Find the minimum and maximum values in X
        for (double val : X) {
            if (val < minVal) {
                minVal = val;
            }
            if (val > maxVal) {
                maxVal = val;
            }
        }

        int n = 10; // Choose a suitable n
        List<double[]> dyadicCover = new ArrayList<>();

        for (int k = (int) (minVal * Math.pow(2, n)); k <= (int) (maxVal * Math.pow(2, n)); k++) {
            double intervalStart = k / Math.pow(2, n);
            double intervalEnd = (k + 1) / Math.pow(2, n);
            boolean intersects = false;
            for (double val : X) {
                if (val >= intervalStart && val < intervalEnd) {
                    intersects = true;
                    break;
                }
            }
            if (intersects) {
                dyadicCover.add(new double[]{intervalStart, intervalEnd});
            }
        }

        return dyadicCover;
    }

    public static void main(String[] args) {
        double[] X = {0.1, 0.3, 0.6, 1.2, 1.5};
        List<double[]> dyadicCover = computeDyadicCover(X);
        for (double[] interval : dyadicCover) {
            System.out.println("[" + interval[0] + ", " + interval[1] + "]");
        }
    }
}
