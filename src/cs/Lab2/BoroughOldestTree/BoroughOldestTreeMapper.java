package cs.Lab2.BoroughOldestTree;

import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.json.simple.*;

public class BoroughOldestTreeMapper extends Mapper<LongWritable, Text, Text, Text> {
	private JSONObject yearBoroughObject = new JSONObject();
	private Text yearBoroughText = new Text();
	private Text allTrees = new Text("All trees");
	
	// Overriding of the map method
	// Input : (lineNumber, lineOfCSV)
	// Output : (All Trees, TextJSONWithBoroughAndYear)
	@SuppressWarnings("unchecked")
	@Override
	protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
    {
		// The first line of the the input file has no relevant information
		if (keyE.get() != 0)
		{
			// Split the information in one line
			String[] lineSplit = valE.toString().split(";");
			
			// If we know the borough and the year
        	if (!lineSplit[5].isEmpty() && !lineSplit[1].isEmpty())
        	{
        		// Recover the year and put in in a JSON
        		Long year = Long.parseLong(lineSplit[5]);
        		yearBoroughObject.put("year",year);
        		
        		// Recover the borough and put in in a JSON
        		yearBoroughObject.put("borough",lineSplit[1]);
        		
        		// Convert the JSON Object to a string
        		yearBoroughText.set(yearBoroughObject.toJSONString());
        		
        		// Output
        		context.write(allTrees, yearBoroughText);
        	}
        	else
        	{
        		return;
        	}
    	}
    }
}