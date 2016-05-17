import java.io.Console;
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

public class SensorStatNotifier {
	String MosquittoBrokerUrl = "tcp://54.213.131.219:2882";
	String MongoDaemonIp = "54.213.131.219";
	int MongoPort = 27018;
	String dbName = "smartagri";
	int previousSensorList = -1;
	int i = 0;
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SensorStatNotifier ssn = new SensorStatNotifier();
		try{
			while(true)
			{
				ssn.GetDataSetsFromMongoDB();
				Thread.sleep(2000);
			}
			//System.in.read();
		}
		catch(Exception ex)
		{
			
		}
	}
	public void GetDataSetsFromMongoDB(){
		MongoClient mongoClient = new MongoClient(MongoDaemonIp,MongoPort);
		MongoDatabase db = mongoClient.getDatabase(dbName);
		FindIterable<Document> iterable = db.getCollection("user").find(/*new Document("Username","User1")*/);
		i = 0;
		iterable.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		    	try{
		    		i++;
		    	}
		    	catch(Exception ex)		    	
		    	{
		    		System.out.println(ex.toString());
		    	}
		    }
		});
		System.out.println(i);
		if(previousSensorList != -1)
		{
			if(previousSensorList < i)
			{
				sendMessage("sensornotify","addSensor");
			}
			else
			if(previousSensorList > i)
			{
				sendMessage("sensornotify","deleteSensor");
			}
		}
		previousSensorList = i;
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

}
