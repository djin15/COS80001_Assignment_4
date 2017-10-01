import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import java.util.HashMap;

public class Reduce extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		String fileName = "";
		String textString = "{";
		HashMap<String, Integer> keyword = new HashMap<String, Integer>();
		for (Text val : values) {
			fileName = val.toString();
			if (keyword.containsKey(fileName))
				keyword.put(fileName, keyword.get(fileName) + 1);
			else
				keyword.put(fileName, 1);
			sum++;

		}
		//print out file names
		context.write(key, new Text(keyword.toString() + "\n"));
		//print out total occurrence
		context.write(null, new Text("Total Occurrence of " + key.toString() + ": " + sum + "\n"));
	}
}