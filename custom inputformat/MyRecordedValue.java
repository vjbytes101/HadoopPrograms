package custominput;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class MyRecordReader extends RecordReader<MyKey,MyValue> {

	private MyKey key;
	private MyValue value;
	private LineRecordReader reader = new LineRecordReader();
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		reader.close();
	}

	@Override
	public MyKey getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public MyValue getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return reader.getProgress();
	}

	@Override
	public void initialize(InputSplit is, TaskAttemptContext tac)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		reader.initialize(is, tac);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		boolean gotNextKeyValue = reader.nextKeyValue();
		if(gotNextKeyValue){
			if(key == null){
				key = new MyKey();
			}
			if(value == null){
				value = new MyValue();
			}
			Text line = reader.getCurrentValue();
			String[] token = line.toString().split("\t");
			key.setSensorType(new Text(token[0]));
			key.setTimestamp(new Text(token[1]));
			key.setStatus(new Text(token[2]));
			value.setValue1(new Text(token[3]));
			value.setValue2(new Text(token[4]));
		}else{
			key = null;
			value = null;
		}
		return gotNextKeyValue;
	}

}
