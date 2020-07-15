package au.edu.rmit.bdp.clustering.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import au.edu.rmit.bdp.clustering.model.Centroid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;

import au.edu.rmit.bdp.clustering.model.DataPoint;
import de.jungblut.math.DoubleVector;

/**
 * calculate a new centroid for these vertices
 */
public class KMeansReducer extends Reducer<Centroid, DataPointWrapper, Centroid, DataPoint> {

	/**
	 * A flag indicates if the clustering converges.
	 */
	public static enum Counter {
		CONVERGED
	}

	private final List<Centroid> centers = new ArrayList<>();

	/**
	 * Having had all the dataPoints, we recompute the centroid and see if it converges by comparing previous centroid (key) with the new one.
	 *
	 * @param centroid 		key
	 * @param dataPoints	value: a list of dataPoints associated with the key (dataPoints in this cluster)
	 */
	@Override
	protected void reduce(Centroid centroid, Iterable<DataPointWrapper> dataPoints, Context context) throws IOException,
			InterruptedException {

		List<DataPoint> vectorList = new ArrayList<>();
		
		System.out.println("Reduce : ");
		System.out.println("Old Centroid : "+centroid);
		
		// compute the new centroid
		DoubleVector newCenter = null;
		for (DataPointWrapper values : dataPoints) {
//			System.out.println("DataPointWrapper : "+values);
			for (DataPoint value : values.getDatapoints()) {
//				System.out.println("DataPoints : "+value);
				vectorList.add(value);
//				System.out.println("newCenter INSIDE: "+newCenter);
				if (newCenter == null)
					newCenter = value.getVector().deepCopy();
				else
					newCenter = newCenter.add(value.getVector());
			}
		}
		
//		System.out.println("vectorList : "+vectorList);
//		System.out.println("newCenter : "+newCenter);
		newCenter = newCenter.divide(vectorList.size());
		Centroid newCentroid = new Centroid(newCenter);
		System.out.println("New Centroid : "+newCenter+" & Datapoints Size : "+vectorList.size());
		centers.add(newCentroid);

		// write new key-value pairs to disk, which will be fed into next round mapReduce job.
		for (DataPoint vector : vectorList) {
			context.write(newCentroid, vector);
		}

		// check if all centroids are converged.
		// If all of them are converged, the counter would be zero.
		// If one or more of them are not, the counter would be greater than zero.
		if (newCentroid.update(centroid))
			context.getCounter(Counter.CONVERGED).increment(1);

	}

	/**
	 * Write the recomputed centroids to disk.
	 */
	@SuppressWarnings("deprecation")
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		Configuration conf = context.getConfiguration();
		Path outPath = new Path(conf.get("centroid.path"));
		FileSystem fs = FileSystem.get(conf);
		
//		if(centers.size() > 0)
		fs.delete(outPath, true);
		
		System.out.println("Reduce Cleanup : ");
		System.out.println("Num Centroids : "+centers.size());
		
		try (SequenceFile.Writer out = SequenceFile.createWriter(fs, context.getConfiguration(), outPath,
				Centroid.class, IntWritable.class)) {

			//write updated centroids to centroid.seq file path
			int index = 0;
			IntWritable result = new IntWritable();
			for (Centroid center : centers) {
				index++;
				center.setClusterIndex(index++);
				result.set(index);
				out.append(center, result);
				System.out.println("New Centroid : "+center);
			}

		}
	}
}
