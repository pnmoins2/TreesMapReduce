package cs.Lab2.HeightHighestTreeByType;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class HeightHighestTreeByTypeReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
	private FloatWritable heightHighestTreeByType = new FloatWritable();
	
    	// Overriding of the reduce function
	// Input : (treeType, list of Heights)
	// Output : (treeType, highestHeight)
    	@Override
    	protected void reduce(final Text cleI, final Iterable<FloatWritable> listevalI, final Context context) throws IOException,InterruptedException
    	{
		// Initiate the local variables
		float highestHeight = 0;
		float currentHeight;

		// For each tree 
		Iterator<FloatWritable> iterator = listevalI.iterator();
		while (iterator.hasNext())
		{
			// Recover the height
			currentHeight = iterator.next().get();

			// If the current height is higher than the current highest height, we update the information
			if (currentHeight > highestHeight)
			{
				highestHeight = currentHeight;
			}
		}

		// Output
		heightHighestTreeByType.set(highestHeight);
		context.write(cleI,heightHighestTreeByType);
    	}

}
