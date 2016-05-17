import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.Scanner;
import org.bson.Document;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.text.*;
public class SensorDataCollector implements MqttCallback{
	MqttClient client;
	String MosquittoBrokerUrl = "tcp://54.213.131.219:2882";
	String MongoDaemonIp = "54.213.131.219";
	int MongoPort = 27018;
	String dbName = "smartagri";
	public SensorDataCollector() {
	}

	public static void main(String[] args) {
	    new SensorDataCollector().doDemo();
	}

	public void doDemo() {
	    try {
	        client = new MqttClient(MosquittoBrokerUrl, "smartagidatacollector");
	        client.connect();
	        client.setCallback(this);
	        client.subscribe("/data/#");
	        System.in.read();
	        client.disconnect();
	        client.close();
	    } catch (MqttException e) {
	        e.printStackTrace();
	    } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void connectionLost(Throwable cause) {
	    // TODO Auto-generated method stub

	}

	@Override
	public void messageArrived(String topic, MqttMessage message)
	        throws Exception {
		try{
		String[] topicParser = topic.split("/");
		System.out.println("User:"+topicParser[2] +" Sensorname:" + topicParser[3] + " SensorId:" + topicParser[4]);
		System.out.println(topic);
		System.out.println(message);   
		JSONObject jsonDoc = new JSONObject(message.toString());
		double Latitude = jsonDoc.getDouble("Latitude");
		double Longitude = jsonDoc.getDouble("Longitude");
		String user = jsonDoc.getString("Username");
		String sensorName = jsonDoc.getString("Sensor");
		int sensorId = jsonDoc.getInt("SensorId");
    	String data =  jsonDoc.getString("Data");
		String dataunit = jsonDoc.getString("DataUnit");
    	int threshold = jsonDoc.getInt("Threshold");
		int frequency = jsonDoc.getInt("Frequency");
		insertDataToMongoDB(user, sensorName, Latitude, Longitude, sensorId, data, dataunit);
		}
		catch(Exception ex){
			System.out.println(ex.toString());
		}
		//todo update mongo
	}
	public void insertDataToMongoDB(String username , String sensorname ,Double Lat , Double Long,int sensorid , String value , String units)
	{
		//MongoClient mongoClient = new MongoClient("54.191.239.166",27018);
		
		System.out.println("insert data to mongo db");
		java.util.Date date= new java.util.Date();
        String time = new Timestamp(date.getTime()).toString();
		MongoClient mongoClient = new MongoClient(MongoDaemonIp,MongoPort);
		MongoDatabase db = mongoClient.getDatabase(dbName);
		System.out.println(username);
		Date now = new Date();

		BasicDBObject timeNow = new BasicDBObject("date", now);
		Document d = new Document()
				.append("User", username)
                .append("Sensorname", sensorname)
                .append("SensorId", sensorid)
                .append("Latitude", Lat)
                .append("Longitude", Long)
                .append("Data", value)
                .append("Units", units);
        d.putAll(timeNow);
		db.getCollection(username).insertOne(d);		
		mongoClient.close();
	}
	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
	    // TODO Auto-generated method stub
		
	}
}
