package hadoopjoinexample;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

public class Test {

	
	//This is the class for "key" for mapper
	public static class ProductIdKey implements WritableComparable<ProductIdKey>{

		public IntWritable productId = new IntWritable();
		public IntWritable recordType = new IntWritable();//0 = productRecord, 1 = orderDetails
		
		public static final IntWritable PRODUCT_RECORD = new IntWritable(0);
		public static final IntWritable DATA_RECORD = new IntWritable(1);
		
		public ProductIdKey(){}
		public ProductIdKey(int productId, IntWritable recordType) {
		    this.productId.set(productId);
		    this.recordType = recordType;
		}
		
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			 this.productId.readFields(in);
			 this.recordType.readFields(in);
		}

		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			this.productId.write(out);
		    this.recordType.write(out);
		}

		public int compareTo(ProductIdKey other) {
			// TODO Auto-generated method stub
			  if (this.productId.equals(other.productId )) {
		        return this.recordType.compareTo(other.recordType);
		    } else {
		        return this.productId.compareTo(other.productId);
		    }
		}
		
		public boolean equals (ProductIdKey other) {
		    return this.productId.equals(other.productId) && this.recordType.equals(other.recordType );
		}

		public int hashCode() {
		    return this.productId.hashCode();
		}
		
		
		
		
		
	}
	
	public static class SalesOrderDataMapper extends Mapper<LongWritable, Text, ProductIdKey, JoinGenericWritable>{
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {                           
	        String[] recordFields = value.toString().split("\\t");
	        int productId = Integer.parseInt(recordFields[4]);
	        int orderQty = Integer.parseInt(recordFields[3]);
	        double lineTotal = Double.parseDouble(recordFields[8]);
	                                               
	        ProductIdKey recordKey = new ProductIdKey(productId, ProductIdKey.DATA_RECORD);
	        SalesOrderDataRecord record = new SalesOrderDataRecord(orderQty, lineTotal);
	                                               
	        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
	        context.write(recordKey, genericRecord);
	    }
	}
	               
	public static class ProductMapper extends Mapper<LongWritable, Text, ProductIdKey, JoinGenericWritable>{
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] recordFields = value.toString().split("\\t");
	        int productId = Integer.parseInt(recordFields[0]);
	        String productName = recordFields[1];
	        String productNumber = recordFields[2];
	                                               
	        ProductIdKey recordKey = new ProductIdKey(productId, ProductIdKey.PRODUCT_RECORD);
	        ProductRecord record = new ProductRecord(productName, productNumber);
	        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
	        context.write(recordKey, genericRecord);
	    }
	}
	
	public static class SalesOrderDataRecord implements Writable {
	    public IntWritable orderQty = new IntWritable();
	    public DoubleWritable lineTotal = new DoubleWritable();              

	    public SalesOrderDataRecord(){}              

	    public SalesOrderDataRecord(int orderQty, double lineTotal) {
	        this.orderQty.set(orderQty);
	        this.lineTotal.set(lineTotal);
	    }

	    public void write(DataOutput out) throws IOException {
	        this.orderQty.write(out);
	        this.lineTotal.write(out);
	    }

	    public void readFields(DataInput in) throws IOException {
	        this.orderQty.readFields(in);
	        this.lineTotal.readFields(in);
	    }
	}
	
	public static class ProductRecord implements Writable {

	    public Text productName = new Text();
	    public Text productNumber = new Text();

	    public ProductRecord(){}
	               
	    public ProductRecord(String productName, String productNumber){
	        this.productName.set(productName);
	        this.productNumber.set(productNumber);
	    }

	    public void write(DataOutput out) throws IOException {
	        this.productName.write(out);
	        this.productNumber.write(out);
	    }

	    public void readFields(DataInput in) throws IOException {
	        this.productName.readFields(in);
	        this.productNumber.readFields(in);
	    }
	}

	@SuppressWarnings("unchecked")
	public static class JoinGenericWritable extends GenericWritable {
        
	    private static Class<? extends Writable>[] CLASSES = null;

	    static {
	        CLASSES = (Class<? extends Writable>[]) new Class[] {
	                SalesOrderDataRecord.class,
	                ProductRecord.class
	        };
	    }
	   
	    public JoinGenericWritable() {}
	   
	    public JoinGenericWritable(Writable instance) {
	        set(instance);
	    }

	    @Override
	    protected Class<? extends Writable>[] getTypes() {
	        return CLASSES;
	    }
	}
	
	public static class JoinRecuder extends Reducer<ProductIdKey, JoinGenericWritable, NullWritable, Text>{
	    public void reduce(ProductIdKey key, Iterable<JoinGenericWritable> values, Context context) throws IOException, InterruptedException{
	        StringBuilder output = new StringBuilder();
	        int sumOrderQty = 0;
	        double sumLineTotal = 0.0;
	                                               
	        for (JoinGenericWritable v : values) {
	            Writable record = v.get();
	            if (key.recordType.equals(ProductIdKey.PRODUCT_RECORD)){
	                ProductRecord pRecord = (ProductRecord)record;
	                output.append(Integer.parseInt(key.productId.toString())).append(", ");
	                output.append(pRecord.productName.toString()).append(", ");
	                output.append(pRecord.productNumber.toString()).append(", ");
	            } else {
	                SalesOrderDataRecord record2 = (SalesOrderDataRecord)record;
	                sumOrderQty += Integer.parseInt(record2.orderQty.toString());
	                sumLineTotal += Double.parseDouble(record2.lineTotal.toString());
	            }
	        }
	        
	        if (sumOrderQty > 0) {
	            context.write(NullWritable.get(), new Text(output.toString() + sumOrderQty + ", " + sumLineTotal));
	        }
	    }
	}
	
	
	public static class JoinGroupingComparator extends WritableComparator {
	    public JoinGroupingComparator() {
	        super (ProductIdKey.class, true);
	    }                             

	    @Override
	    public int compare (WritableComparable a, WritableComparable b){
	        ProductIdKey first = (ProductIdKey) a;
	        ProductIdKey second = (ProductIdKey) b;
	                      
	        return first.productId.compareTo(second.productId);
	    }
	}
	
	public static class JoinSortingComparator extends WritableComparator {
	    public JoinSortingComparator()
	    {
	        super (ProductIdKey.class, true);
	    }
	                               
	    @Override
	    public int compare (WritableComparable a, WritableComparable b){
	        ProductIdKey first = (ProductIdKey) a;
	        ProductIdKey second = (ProductIdKey) b;
	                                 
	        return first.compareTo(second);
	    }
	}
	
	
}
