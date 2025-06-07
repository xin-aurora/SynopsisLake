package utils;

import java.util.*;

/**
 * The code is from https://www.geeksforgeeks.org/voronoi-diagram/
 */
//Class to represent a point in 2D space
class Point implements Comparable<Point> {
	double x, y;

	public Point(double x, double y) {
		this.x = x;
		this.y = y;
	}

	// Implement compareTo method for sorting points
	@Override
	public int compareTo(Point other) {
		return Double.compare(this.x, other.x);
	}
}

//Class to represent a Voronoi region
class Region {
	Point site;
	List<Point> vertices;

	public Region(Point site, List<Point> vertices) {
		this.site = site;
		this.vertices = vertices;
	}
}

//Class to represent an Event during the sweep line algorithm
class Event implements Comparable<Event> {
	Point point;
	int index;
	boolean isSite;

	public Event(Point point, int index, boolean isSite) {
		this.point = point;
		this.index = index;
		this.isSite = isSite;
	}

	// Custom comparison for event sorting
	@Override
	public int compareTo(Event other) {
		if (this.point.x == other.point.x) {
			return this.isSite ? -1 : 1;
		}
		return Double.compare(this.point.x, other.point.x);
	}
}

public class VoronoiSweepLine {
	public static List<Region> voronoiSweepLine(List<Point> points) {
		int n = points.size();
		List<Region> regions = new ArrayList<>();

		// Sort points by their x-coordinates
		Collections.sort(points);

		TreeSet<Event> eventQueue = new TreeSet<>();
		for (int i = 0; i < n; i++) {
			eventQueue.add(new Event(points.get(i), i, true));
		}

		while (!eventQueue.isEmpty()) {
			Event currentEvent = eventQueue.pollFirst();

			if (currentEvent.isSite) {
				// Handle site event
				// Implement site event logic here
			} else {
				// Handle circle event
				// Implement circle event logic here
			}

			// Add an empty region to the result for demonstration
			regions.add(new Region(new Point(0, 0), new ArrayList<>()));
		}

		return regions;
	}

	public static void main(String[] args) {
		// Sample input points
		List<Point> points = Arrays.asList(new Point(2, 5), new Point(4, 5), new Point(7, 2), new Point(5, 7));

		// Construct Voronoi Diagram
		List<Region> voronoiRegions = voronoiSweepLine(points);

		// Display Voronoi regions
		for (int i = 0; i < voronoiRegions.size(); i++) {
			Region region = voronoiRegions.get(i);
			System.out.println("Voronoi Region #" + (i + 1) + ": Site (" + region.site.x + ", " + region.site.y + ")");
			for (Point vertex : region.vertices) {
				System.out.println("Vertex (" + vertex.x + ", " + vertex.y + ")");
			}
			System.out.println();
		}
	}
}
