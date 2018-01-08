package cs.Lab2.TreeCountByType;

import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TreeCountByTypeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private IntWritable one = new IntWritable(1);
	private Text typeTree = new Text();
	
	// Overriding of the map method
	// Input : (lineNumber, lineOfCSV)
	// Output : (treeType, 1) 
	@Override
	protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
    {
		// The first line of the the input file has no relevant information
		if (keyE.get() != 0)
		{
			// Split the information in one line
			String[] lineSplit = valE.toString().split(";");
	        
			// Recover the tree type
	        typeTree.set(lineSplit[2]);
	        
	        // Output
	        context.write(typeTree, one);
		}     
    }
}