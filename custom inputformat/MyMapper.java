package custominput;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<MyKey, MyValue, Text, Text>{

	protected void map(MyKey key, MyValue value, Context context) throws java.io.IOException, InterruptedException {
		String sensortype = key.getSensorType().toString();
		
		if(sensortype.toLowerCase().equals("a")){
			context.write(value.getValue1(),value.getValue2());
		}
	}
}
