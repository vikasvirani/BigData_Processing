package au.edu.rmit.bdp.clustering.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
//import java.util.ArrayList;
import java.util.List;

import org.apache.giraph.utils.ArrayListWritable;
//import org.apache.hadoop.io.ArrayWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.io.WritableComparable;

import au.edu.rmit.bdp.clustering.model.DataPoint;

public class DataPointWrapper extends ArrayListWritable<DataPoint>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7501859816723560392L;
//	List<DataPoint> datapoints;
			
	public DataPointWrapper() {
//		super(DataPoint.class);
		super();
		setClass();
//		datapoints = new ArrayList<DataPoint>();
	}
	
	//Constructor to initialize object with passed data type
	public DataPointWrapper(Class<DataPoint> refClass) {
		super(refClass);
//		setClass();
//		datapoints = new ArrayList<DataPoint>();
//		this.datapoints = datapoints;

	}
	
	public DataPointWrapper(ArrayListWritable<DataPoint> arrayListWritable) {
		super(arrayListWritable);
	}
	
	//method to return List of all DataPoints in a Wrapper 
	public List<DataPoint> getDatapoints() {
		List<DataPoint> listIterator = new ArrayList<DataPoint>();
		int size = super.size();
		for(int i = 0; i< size; i++){
			DataPoint dp = super.get(i);
			listIterator.add(dp);
		}
		return listIterator;
	}
	
//
//	public void setDatapoints(List<DataPoint> datapoints) {
//		this.datapoints = datapoints;
//	}
	
	//Overridden method from superclass method
	@Override
	public final void write(DataOutput out) throws IOException {
		super.write(out);
	}
	
	//Overridden method from superclass method
	@Override
	public final void readFields(DataInput in) throws IOException {
		super.readFields(in);
	}

//	@Override
//	public final int compareTo(DataPointWrapper o) {
//		
//		int length = this.datapoints.size();
//		int total = 0;
//		List<DataPoint> original = this.datapoints;
//		List<DataPoint> new_dp = o.getDatapoints();
//		
//		for(int i=0; i< length; i++) {
//			int val = original.get(i).compareTo(new_dp.get(i));
//			total += val;
//		}
//		
//		return total;
//	}
	
	//Overridden method from superclass method
	@Override
	public boolean add(DataPoint e) {
		// TODO Auto-generated method stub
//		this.datapoints.add(e);
		return super.add(e);
	}

	//Overridden method from superclass method
	@Override
	public void setClass() {
		// TODO Auto-generated method stub
		
		super.setClass(DataPoint.class);
	}
	
}
