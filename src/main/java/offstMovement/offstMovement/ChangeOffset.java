package offstMovement;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarFile;

import kafka.producer.KafkaLog4jAppender;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class ChangeOffset {

	private static ZkClient client;
	String CONSUMERS_PATH = "/consumers";
	String BROKER_IDS_PATH = "/brokers/ids";
	static String BROKER_TOPICS_PATH = "/brokers/topics";
	
	String topicSelctd="";
	String brkrSelctd="";
	Integer prtnSlctd;	    
	Map<String, String> brokers ;
	
	
	static class StringSerializer implements ZkSerializer 
	{

        public StringSerializer() {}
        public Object deserialize(byte[] data) throws ZkMarshallingError {
            if (data == null) return null;
            return new String(data);
        }

        public byte[] serialize(Object data) throws ZkMarshallingError {
            return data.toString().getBytes();
        }
    }

	  private String getOffsetsPath(String group) {
	      String brokerId= "0";  
		  return CONSUMERS_PATH + "/" + group + "/offsets/" + topicSelctd +"/"+this.brkrSelctd+"-"+prtnSlctd;
	  }
	
    private List<String> getChildrenParentMayNotExist(String path) {
        try {
            List<String> children = client.getChildren(path);
            return children;
        } catch (ZkNoNodeException e) {
            return new ArrayList<String>();
        }
    }
    
  /*/brokers/topics/topic1/lst_of_brokers for the topic */
    
    public List<String> getTopics(String path)
    {
    	   List<String> topics = getChildrenParentMayNotExist(path);
    	   return topics;
    		
    }
    
    
    public List<String> getBrokers(String path) {
        
        List<String> brokers = getChildrenParentMayNotExist( path);
        return brokers;
    }
    
    
    public Integer getBrokerData(String path)
    {
    	Integer	parts = Integer.parseInt((String)client.readData(path));

      
    	return parts;
    }
    
    
    
    
    public static void processDumps(String path, String logsPath,String topic,String partition,String dumpPath) throws IOException  
    {
        
    	String procPath = logsPath+"/"+topic+"-"+partition+"/";
    	
    	/*Dump the logs for each file in the logs path*/
    	
    	File folder = new File(procPath);
    	File[] lstFiles = folder.listFiles();
    	
    	
    	for(File file :lstFiles)
    	{
    		String fil = file.getCanonicalPath();
    		ProcessBuilder pb = new ProcessBuilder(path+"/bin/kafka-run-class.sh","kafka.tools.DumpLogSegments",fil ) ;
        	String line; 
            Process process=pb.start(); 
            InputStream is = process.getInputStream();
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
         
            File f = new File(dumpPath);
            
            FileWriter fw = new FileWriter( f.getAbsoluteFile(),true);
            BufferedWriter bw = new BufferedWriter(fw);
             
            System.out.println("Started dumping files");
            
            while ((line = br.readLine()) != null)
            {
               // System.out.println(line);
            	bw.write(line+"\n");
            }
            bw.close();
            System.out.println("Dumping complete");
          
           
    		
    	}
    	
    }
    
    public void writeNewOffset(String group, String topic, String partition,long offset)
    {
    	String path = getOffsetsPath(group);
    
    	
    	
    	client.writeData(path, offset);
    }
    
   public static void main(String[] args) throws NumberFormatException, IOException {
	
		// TODO Auto-generated method stub

		Properties props = new Properties();
		props.load(ChangeOffset.class.getClassLoader().getResourceAsStream("config.properties"));
		
		
		String zk= props.getProperty("zookeeperConnect");
		String kLogs = props.getProperty("kafkaLogsDir");
		String kafka = props.getProperty("kafkaExeDir");
		String dumpOpt = props.getProperty("dumpOutputTo");
		String grpName = props.getProperty("group");
		
	
		try
		{
			client = new ZkClient(zk, 6000,10000 , new StringSerializer() );
		}
		catch(Exception ex)
		{
			System.out.println("Unable to connect to zookeeper");
		}
		
		
		
		
		System.out.println("connected to zookeeper");
		
		ChangeOffset obj = new ChangeOffset();
		List<String> topics=obj.getTopics(BROKER_TOPICS_PATH);
		
		int index =1;
		for(String s :topics)
		{
			System.out.println(index+". "+s);
			index++;
					
		}
		index=1;
		
		System.out.println("\n***************************************");
		System.out.println("Select a topic index from the list");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		Integer topicIndex = Integer.parseInt(br.readLine())-1;
		obj.topicSelctd = topics.get(topicIndex);
		
		List<String> brokers=obj.getBrokers(BROKER_TOPICS_PATH+"/"+obj.topicSelctd);
		
		
		System.out.println("Brokers for the selected topic are");
		for(String s :brokers)
		{
			System.out.println("Broker"+index+")"+s);
			index++;
		}
		index=1;
		
		System.out.println("\n***************************************");
		System.out.println("Select a index for broker");
		Integer brkrIndex = Integer.parseInt(br.readLine())-1;
		obj.brkrSelctd = brokers.get(brkrIndex);
		System.out.println("broker index selected is "+ brkrIndex);
		
		Integer brkrPartitons = obj.getBrokerData(BROKER_TOPICS_PATH+"/"+obj.topicSelctd+"/"+obj.brkrSelctd);
		
		System.out.println("***************************************");
		System.out.println("The following partitions are available on "+ " "+obj.brkrSelctd);
		
		for (int i =0;i<brkrPartitons;i++)
		{
			System.out.println(index+")"+"Partition"+i);
			index++;
		}
		
		index=1;
		
		System.out.println("***************************************");
		System.out.println("Select a partition by picking the index");
		Integer prtnSlctd = Integer.parseInt(br.readLine())-1;
		obj.prtnSlctd = prtnSlctd;
				
		
		
		System.out.println("Dumping log segments");
		
		/*Dumping files for the topic-partition */
		
		String prt= obj.prtnSlctd.toString();
		obj.processDumps(kafka,kLogs,obj.topicSelctd,prt,dumpOpt);
		
		DumpExtraction d = new DumpExtraction();
		 Long in =d.dumpToNumbers();
		
		obj.writeNewOffset(grpName, obj.topicSelctd, obj.prtnSlctd.toString(), in);
		
	
		
		
	}

}
