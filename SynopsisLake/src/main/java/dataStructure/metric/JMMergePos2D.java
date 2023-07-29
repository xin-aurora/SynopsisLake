package dataStructure.metric;

import java.util.ArrayList;
import java.util.HashMap;

public class JMMergePos2D {
	public ArrayList<JMTargetCell> mergedtarget = new ArrayList<JMTargetCell>();
	public ArrayList<Integer> mIds = new ArrayList<Integer>();
	public ArrayList<Integer> lefts = new ArrayList<Integer>();
	public ArrayList<Integer> rights = new ArrayList<Integer>();
	public ArrayList<Integer> tops = new ArrayList<Integer>();
	public ArrayList<Integer> bottoms = new ArrayList<Integer>();
	public double mergeDiff = 0;
	public boolean isLeftRight = false;

	public JMMergePos2D(boolean isLeftRight, ArrayList<Integer> lefts, ArrayList<Integer> rights,
			ArrayList<Integer> tops, ArrayList<Integer> bottoms, int id, HashMap<Integer, JMTargetCell> idToCellMap) {
		this.isLeftRight = isLeftRight;
		if (isLeftRight) {
			// merge left right
			// keep top bottom stay
			for (int i = 0; i < lefts.size(); i++) {
				JMTargetCell left = idToCellMap.get(lefts.get(i));
				JMTargetCell right = idToCellMap.get(rights.get(i));
				double weight = (left.weight + right.weight) / 2;
				JMTargetCell target = new JMTargetCell(left.minLon, right.maxLon, left.minLat, left.maxLat, weight,
						(id + i));
				mIds.add(target.id);
				target.addAllOverlap(left.overlappings);
				target.addAllOverlap(right.overlappings);
				target.computeScore();
				target.lIds.addAll(left.lIds);
				target.rIds.addAll(right.rIds);
				target.tIds.addAll(left.tIds);
				target.tIds.addAll(right.tIds);
				target.bIds.addAll(left.bIds);
				target.bIds.addAll(right.bIds);
				this.lefts = lefts;
				this.rights = rights;
				mergeDiff += (left.score + right.score) - target.score;
				this.mergedtarget.add(target);
			}

//			System.out.println("left size = " + lefts.size() + ", right size = " + rights.size() + ", merged size = " + mergedtarget.size());

		} else {
			// keep left right stay
			// merge top bottom
			for (int i = 0; i < tops.size(); i++) {
				JMTargetCell top = idToCellMap.get(tops.get(i));

				JMTargetCell bottom = idToCellMap.get(bottoms.get(i));
				double weight = (top.weight + bottom.weight) / 2;
				JMTargetCell target = new JMTargetCell(bottom.minLon, bottom.maxLon, bottom.minLat, top.maxLat, weight,
						(id + i));
				mIds.add(target.id);
				target.addAllOverlap(top.overlappings);
				target.addAllOverlap(bottom.overlappings);
				target.computeScore();
				target.lIds.addAll(top.lIds);
				target.lIds.addAll(bottom.lIds);
				target.rIds.addAll(top.rIds);
				target.rIds.addAll(bottom.rIds);
				target.tIds.addAll(top.tIds);
				target.bIds.addAll(bottom.bIds);
				this.tops = tops;
				this.bottoms = bottoms;
				mergeDiff += (top.score + bottom.score) - target.score;
				this.mergedtarget.add(target);
			}
//			System.out.println("Top size = " + tops.size() + ", bottom size = " + bottoms.size() + ", merged size = " + mergedtarget.size());
		}

	}

	@Override
	public final String toString() {
		if (isLeftRight) {
			return "left-right merge";
		} else {
			return "top-bottom merge";
		}
	}
}
