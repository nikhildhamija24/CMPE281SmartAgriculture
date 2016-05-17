import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Random;

import org.bson.Document;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.JSONObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

public class VirtualSensorController {
	
	HashMap<String, String> hmapUnit = new HashMap<String, String>();
	HashMap<String, String> hmapSensors = new HashMap<String, String>();
	HashMap<Integer, String> hmapLMU = new HashMap<Integer, String>();
	String MosquittoBrokerUrl = "tcp://54.213.131.219:2882";
	String MongoDaemonIp = "54.213.131.219";
	int MongoPort = 27018;
	String dbName = "smartagri";
	public VirtualSensorController()
	{
		hmapUnit.put("Organic Carbon", "g/kg");
		hmapUnit.put("Soil Acidity", "ph in H20 * 10");
		hmapUnit.put("Sand Content", "%");
		hmapUnit.put("Silt Content", "%");
		hmapUnit.put("Clay Content", "%");
		hmapUnit.put("Cation Exchange Capability", "cmol/kg");
		hmapUnit.put("Soil Density", "kg/m3");
		hmapUnit.put("SoilMoisture", "g/kg");
		
		hmapSensors.put("Organic Carbon", "ORCDRC");
		hmapSensors.put("Soil Acidity", "PHIHOX");
		hmapSensors.put("Sand Content", "SNDPPT");
		hmapSensors.put("Silt Content", "SLTPPT");
		hmapSensors.put("Clay Content", "CLYPPT");
		hmapSensors.put("Cation Exchange Capability", "CEC");
		hmapSensors.put("Soil Density", "BLD");
		hmapSensors.put("SoilMoisture", "SoilMoisture");
		
		hmapLMU.put(0, "L");
		hmapLMU.put(1, "M");
		hmapLMU.put(2, "U");
		hmapLMU.put(3, "M");
	}
	public void GetDataSetsFromMongoDB(){
		MongoClient mongoClient = new MongoClient(MongoDaemonIp,MongoPort);
		MongoDatabase db = mongoClient.getDatabase(dbName);
		FindIterable<Document> iterable = db.getCollection("user").find(/*new Document("Username","User1")*/);
		
		iterable.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		    	try{
		    		System.out.println("[0]------------------------------------[0]");
			        System.out.println(document.toJson());
			    	String documentContent = document.toJson();
			    	System.out.println("[0.1]------------------------------------[0]");
			    	JSONObject jsonDoc = new JSONObject(documentContent);
			    	System.out.println("[0.2]------------------------------------[0]");
			    	double Lat = jsonDoc.getDouble("latitude");
			    	double Long = jsonDoc.getDouble("longitude");
			    	String user =  jsonDoc.getString("username");
			    	System.out.println("[0.3]------------------------------------[0]");
			    	String interestedSensor = jsonDoc.getString("sensorName");
			    	int sensorId = jsonDoc.getInt("sensorId");
			    	int threshold = jsonDoc.getInt("threshold");
			    	int frequency = jsonDoc.getInt("frequency");
			    	System.out.println("[0.4]------------------------------------[0]");
			    	String retVal = fetchDataSetsFromSoilGrids(Lat, Long, interestedSensor);
			    	System.out.println("[1]------------------------------------[1]");
			    	JSONObject dataToSend = new JSONObject();
			    	dataToSend.put("Latitude", Lat) ;
			    	dataToSend.put("Longitude", Long) ;
			    	dataToSend.put("Username",user) ;
			    	dataToSend.put("Sensor",interestedSensor);
			    	dataToSend.put("SensorId",sensorId);
			    	dataToSend.put("Data",retVal);
			    	dataToSend.put("DataUnit","Kg/Mol");
			    	dataToSend.put("Threshold",threshold);
			    	dataToSend.put("Frequency",frequency);
			    	System.out.println("test " +dataToSend.toString());
			    	String topicName = "/data/"+user+"/"+interestedSensor+"/"+sensorId;
			    	sendMessage(topicName , dataToSend.toString());
			    	System.out.println("[2]------------------------------------[2]");
			    }
		    	catch(Exception ex)		    	
		    	{
		    		System.out.println(ex.toString());
		    	}
		    }
		});
		mongoClient.close();
	}
	public void sendMessage(String topicName , String messageContent)
	{
		 	String topic        = topicName;
	        String content      = messageContent;
	        int qos             = 2;
	        String clientId     = "smartagri";
	        MemoryPersistence persistence = new MemoryPersistence();
	        try {
	            MqttClient sampleClient = new MqttClient(MosquittoBrokerUrl, clientId, persistence);
	            MqttConnectOptions connOpts = new MqttConnectOptions();
	            connOpts.setCleanSession(true);
	            System.out.println("Connecting to broker: "+MosquittoBrokerUrl);
	            sampleClient.connect(connOpts);
	            System.out.println("Connected");
	            System.out.println("Publishing message: "+content);
	            MqttMessage message = new MqttMessage(content.getBytes());
	            message.setQos(qos);
	            sampleClient.publish(topic, message);
	            System.out.println("Message published");
	            sampleClient.disconnect();
	            System.out.println("Disconnected");
	            //System.exit(0);
	        } catch(MqttException me) {
	            System.out.println("reason "+me.getReasonCode());
	            System.out.println("msg "+me.getMessage());
	            System.out.println("loc "+me.getLocalizedMessage());
	            System.out.println("cause "+me.getCause());
	            System.out.println("excep "+me);
	            me.printStackTrace();
	        }
	}

	public String parseLMU(String s){
		Random randomgenerator = new Random();
		int lmuValue = randomgenerator.nextInt(3);
		JSONObject obj = new JSONObject(s);
		String soilMask = obj.getJSONObject(hmapLMU.get(lmuValue)).toString();
		return soilMask;
		
		
	}
	//@SuppressWarnings("finally")
	public String fetchDataSetsFromSoilGrids(double Lat, double Long, String sensor)
	{
		String sensorData = "";
		sensor = hmapSensors.get(sensor);
		try {
			if(sensor.equalsIgnoreCase("SoilMoisture")){
				Random random = new Random();
				String SoilMoistureGen = "{\"sd1\": 42, \"sd2\": 43, \"sd3\": 44, \"sd4\": 45, \"sd5\": 46, \"sd6\": 48 }";
				//System.out.println(SoilMoistureGen);
				return SoilMoistureGen;
			}
			if(sensor.equalsIgnoreCase("PH"))
				sensor = "PHIHOX";
			String url = "http://rest.soilgrids.org/query?lon="+Long+"&lat=" + Lat + "&attributes=" + sensor;
			//System.out.println(url);
			URLConnection yc;
			URL link = new URL(url);
			yc = link.openConnection();
			InputStream is = yc.getInputStream();				
		    //FileOutputStream fos = new FileOutputStream(fileName);
		    int contentRead = 0;
		    byte[] contentBuffer = new byte[32768];	
		    String fullContent = "";
		    while( (contentRead = is.read(contentBuffer)) > 0) {
		    	String utfString = new String(contentBuffer, "UTF8");
		    	fullContent = fullContent + utfString;
		    	//System.out.println(utfString);
		    }
		    JSONObject obj = new JSONObject(fullContent);
	    	String soilMask = obj.getJSONObject("properties").getString("soilmask");
	    	if(soilMask.equals("soil")){
	    		sensorData = obj.getJSONObject("properties").getJSONObject(sensor).toString();
	    		
	    		//System.out.println(sensor);
	    	}
	    	else
	    	{
	    		Random random = new Random();
	    		sensorData = "{\"sd1\": 42, \"sd2\": 43, \"sd3\": 44, \"sd4\": 45, \"sd5\": 46, \"sd6\": 48 }";
				
	    	}
		    is.close();
		    sensorData = parseLMU(sensorData);
		    return sensorData;
		} catch (Exception e) {
			Random random = new Random();
    		sensorData = "{\"sd1\": 42, \"sd2\": 43, \"sd3\": 44, \"sd4\": 45, \"sd5\": 46, \"sd6\": 48 }";
    		sensorData = parseLMU(sensorData);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			System.out.println("inside finally exception");
			
		}
		return sensorData;
	//	return "";
	}
	public static void main(String[] args) {
		
		try{
		VirtualSensorController vsc = new VirtualSensorController();
			while(true)
			{
				vsc.GetDataSetsFromMongoDB();
				Thread.sleep(3000);
			}
		}
		catch(Exception ex)
		{
			
		}
		//new VirtualSensorController().GetDataSetsFromMongoDB();
		
		//for(int i = 0 ; i < 5 ; i++){
		/*	String retString = vsc.fetchDataSetsFromSoilGrids(51.57, 5.39, "Organic Carbon");
			System.out.println(retString);
			
			retString = vsc.fetchDataSetsFromSoilGrids(51.57, 5.39, "Sand Content");
			System.out.println(retString);
			retString = vsc.fetchDataSetsFromSoilGrids(51.57, 5.39, "Silt Content");
			System.out.println(retString);
			retString = vsc.fetchDataSetsFromSoilGrids(51.57, 5.39, "Clay Content");
			System.out.println(retString);
			retString = vsc.fetchDataSetsFromSoilGrids(51.57, 5.39, "Cation Exchange Capability");
			System.out.println(retString);
			retString = vsc.fetchDataSetsFromSoilGrids(51.57, 5.39, "Soil Density");
			System.out.println(retString);
			retString = vsc.fetchDataSetsFromSoilGrids(51.57, 5.39, "Soil Acidity");
			System.out.println(retString);
		//}*/
			
	}
	
}
