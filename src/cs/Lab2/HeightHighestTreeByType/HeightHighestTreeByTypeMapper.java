package cs.Lab2.HeightHighestTreeByType;

import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HeightHighestTreeByTypeMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	private FloatWritable height = new FloatWritable();
	private Text typeTree = new Text();
	
	// Overriding of the map method
	// Input : (lineNumber, lineOfCSV)
	// Output : (treeType, heightOfTree)
	@Override
	protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
    	{
		// The first line of the the input file has no relevant information
		if (keyE.get() != 0)
		{
			// Split the information in one line
			String[] lineSplit = valE.toString().split(";");
			
			// If there is the theight of the tree
			if (!lineSplit[6].isEmpty())
			{
				// Recover the tree type
				typeTree.set(lineSplit[2]);

				// Recover the height of the tree
				height.set(Float.parseFloat(lineSplit[6]));

				// Output
				context.write(typeTree, height);
			}
			else
			{
				return;
			}
    		}
    	}
}
