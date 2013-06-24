package offstMovement;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.io.FileReader;

public class DumpExtraction {

	
	public Long dumpToNumbers() throws IOException
	{
	
		BufferedReader br =null;
		List<Long> offstNumbers = new ArrayList<Long>();
		
		Properties props = new Properties();
		props.load(ChangeOffset.class.getClassLoader().getResourceAsStream("config.properties"));
		 
		String dumps = props.getProperty("dumpOutputTo");
				
		
		
		  try
		  {
			  String sCurrentLine;
			  
			  File fr = new File(dumps);
			  
			  br = new BufferedReader(new FileReader(fr.getAbsoluteFile()));
			  while ((sCurrentLine = br.readLine())!=null) {
			
				 if(sCurrentLine.startsWith("offset"))
				 {
					 String[] splitArr= sCurrentLine.split(":");
					 
					 String[] offsts = splitArr[1].split("[ ]+");
					 offstNumbers.add(Long.parseLong(offsts[1]));
				 }
				   
		  }
			 
			 System.out.println("Enter a offset number");
			 br = new BufferedReader( new InputStreamReader(System.in));
			 String offstInput = br.readLine();
			 
			 
			 System.out.println("Input from the user offset is "+ offstInput);
			  long clstOffst = Integer.MAX_VALUE;
			  Integer index=0;
			 for(int i =0; i< offstNumbers.size();i++)
			 {
				 if( Math.abs(Integer.parseInt(offstInput)- offstNumbers.get(i))<clstOffst)
				 {
					 clstOffst = Math.abs(Long.parseLong(offstInput)- offstNumbers.get(i));
					 index=i;
				 }
				 
			 }
			 
			 System.out.println("Closest available offset to is "+ offstNumbers.get(index));
			
			 br.close();
			 Long in =  (Long)offstNumbers.get(index);
			 
			 return in;
			 
		  }
		  
			 
		  catch(Exception e)
		  {
			  
		  }
		return null;
		  
	}
}

	
