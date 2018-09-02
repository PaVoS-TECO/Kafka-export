package edu.teco.pavos.pke;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Implementation of the FileWriterStrategy interface for CSV files.
 * @author Jean Baumgarten
 */
public class CSVWriterStrategy implements FileWriterStrategy {

    private HashSet<String> features = new HashSet<String>();
    private HashSet<String> dataStreams = new HashSet<String>();
    private HashSet<String> locations = new HashSet<String>();
    private HashSet<String> things = new HashSet<String>();
    private HashSet<String> sensors = new HashSet<String>();
    private HashSet<String> observedProperties = new HashSet<String>();
	private DateTimeFormatter timeParser;
	private Set<String> obsProps;
	private JSONParser jsonParser;
	private Set<String> clusters;
	private int clusterDepth;
	private PrintWriter writer;
	private TimeIntervall interval;

	/**
     * Default constructor
     * @param props are the properties of the data, that should be exported to a File.
     */
    public CSVWriterStrategy(ExportProperties props) {
    	
    	this.jsonParser = new JSONParser();
    	this.interval = props.getTimeFrame();
		this.timeParser = ISODateTimeFormat.dateTime();
		this.obsProps = props.getObservedProperties();
		this.clusters = props.getClusters();
		for (String s : this.clusters) {
			String[] parts = s.split(":");
			String id = parts[1];
			String[] parts2 = id.split("-");
			this.clusterDepth = parts2.length;
			break;
		}
		
    }
    
    /**
     * Creates a File as specified by the FilePath and saves the Data from the provided KafkaStream into it.
     * @param file Is the FilePath, where the new File should be created.
     */
	public void saveToFile(File file) {
		
		try {
			
			this.writer = new PrintWriter(file.getAbsolutePath(), "UTF-8");
			KafkaDataGetter kafka = new KafkaDataGetter();
			boolean work = true;
			
			while (work && kafka.doMoreDataExist()) {
				
				HashSet<JSONObject> data = kafka.getNextData();
				int amount = data.size();
				int out = 0;
				
				for (JSONObject json : data) {
					
					boolean inTimeFrame = processRecord(json);
					if (!inTimeFrame)
						out++;
					
				}
				
				if ((100 * out / amount) > 90)
					work = false;
				
			}
	        
			kafka.close();
			this.writer.close();
			
		} catch (FileNotFoundException e) {
			
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		} catch (UnsupportedEncodingException e) {
			
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}
		
	}
	
	private boolean processRecord(JSONObject obs) {
		
		try {
			
			DateTime time = this.timeParser.parseDateTime("" + obs.get("phenomenonTime"));
			
			if (this.interval.isInside(time)) {

				JSONObject dataStream = (JSONObject) this.jsonParser.parse((String) obs.get("Datastream"));
				String dts = (String) dataStream.get("ObservedProperty");
				JSONObject observedProperty = (JSONObject) this.jsonParser.parse(dts);
				String o = "" + observedProperty.get("name");
				
				if (this.obsProps.contains(o)) {
					
					//TODO get real cluster ID from record
					String foi = (String) obs.get("FeatureOfInterest");
					JSONObject featureOfInterest = (JSONObject) this.jsonParser.parse(foi);
					String cluster = (String) featureOfInterest.get("description");
					
					if (this.isOneOfOrContainedInExportClusters(cluster)) {
						
						JSONObject thing = (JSONObject) this.jsonParser.parse((String) dataStream.get("Thing"));
						JSONObject sensor = (JSONObject) this.jsonParser.parse((String) dataStream.get("Sensor"));
						
						// Location is not used, since created with foi
						// will lead to problem with unnamed locations (iot.id-wise)
						//JSONObject location = (JSONObject) record.get("Location");
						
						if (!this.observedProperties.contains((String) observedProperty.get("iotId")))
							this.writer.println(this.getObservedPropertyLine(observedProperty));
						
						if (!this.sensors.contains((String) sensor.get("iotId")))
							this.writer.println(this.getSensorLine(sensor));
						
						if (!this.things.contains((String) thing.get("iotId")))
							this.writer.println(this.getThingLine(thing));
						
						if (!this.features.contains((String) featureOfInterest.get("iotId")))
							this.writer.println(this.getFeatureLine(featureOfInterest));
						
						if (!this.dataStreams.contains((String) dataStream.get("iotId")))
							this.writer.println(this.getDataStreamLine(dataStream, thing, observedProperty, sensor));
						
						this.writer.println(this.getObservationLine(obs, dataStream, featureOfInterest));
						
					}
					
				}
				
			} else {
				
				return !this.interval.isAfter(time);
				
			}
			
			return true;
			
		} catch (ParseException e) {
			
			// TODO Auto-generated catch block
			e.printStackTrace();
			return true;
			
		}
		
	}
	
	private boolean isOneOfOrContainedInExportClusters(String clusterID) {
		
		String[] parts = clusterID.split(":");
		String id = parts[1];
		String[] parts2 = id.split("-");
		String dID = parts[0] + ":" + parts2[0];
		for (int i = 1; i < this.clusterDepth; i++) {
			dID += "-" + parts2[i];
		}
		return this.clusters.contains(dID);
		
	}
	
	private String getObservedPropertyLine(JSONObject op) {
		
		this.observedProperties.add(op.get("iotId").toString());
		
		String line = "observedPropertyϢ" + op.get("iotId") + "Ϣ";
		line += op.get("name") + "Ϣ";
		line += op.get("description") + "Ϣ";
		line += op.get("definition");
		
		return line;
		
	}
	
	private String getSensorLine(JSONObject s) {
		
		this.sensors.add(s.get("iotId").toString());
		
		String line = "sensorϢ" + s.get("iotId") + "Ϣ";
		line += s.get("name") + "Ϣ";
		line += s.get("description") + "Ϣ";
		line += s.get("encodingType") + "Ϣ";
		line += s.get("metadata");
		
		return line;
		
	}
	
	private String getLocationLine(JSONObject l) {
		
		this.locations.add(l.get("iotId").toString());
		
		String line = "locationϢ" + l.get("iotId") + "Ϣ";
		line += l.get("name") + "Ϣ";
		line += l.get("description") + "Ϣ";
		line += l.get("encodingType") + "Ϣ";
		line += ((JSONObject) l.get("location")).toJSONString();
		
		return line;
		
	}
	
	private String getFeatureLine(JSONObject f) {
		
		this.features.add(f.get("iotId").toString());
		
		String line = "featureOfInterestϢ" + f.get("iotId") + "Ϣ";
		line += f.get("name") + "Ϣ";
		line += f.get("description") + "Ϣ";
		line += f.get("encodingType") + "Ϣ";
		line += ((JSONObject) f.get("feature")).toJSONString();
		
		return line;
		
	}
	
	private String getThingLine(JSONObject t) {
		
		this.things.add(t.get("iotId").toString());
		
		String line = "thingϢ" + t.get("iotId") + "Ϣ";
		line += t.get("name") + "Ϣ";
		line += t.get("description") + "Ϣ";
		
		String opt = "";
		Object optional = t.get("properties");
		if (optional != null) {
			opt = ((JSONObject) optional).toJSONString();
		}
		line += opt;
		
		return line;
		
	}
	
	private String getDataStreamLine(JSONObject d, JSONObject t, JSONObject op, JSONObject s) {
		
		this.dataStreams.add(d.get("iotId").toString());
		
		String line = "dataStreamϢ" + d.get("iotId") + "Ϣ";
		line += d.get("name") + "Ϣ";
		line += d.get("description") + "Ϣ";
		line += d.get("observationType") + "Ϣ";
		line += ((JSONObject) d.get("unitOfMeasurement")).toJSONString() + "Ϣ";
		
		line += t.get("iotId") + "Ϣ";
		line += op.get("iotId") + "Ϣ";
		line += s.get("iotId") + "Ϣ";
		
		String opt = "";
		Object optional = d.get("observedArea");
		if (optional != null) {
			opt = "" + optional;
		}
		line += opt + "Ϣ";
		opt = "";
		optional = d.get("phenomenonTime");
		if (optional != null) {
			opt = "" + optional;
		}
		line += opt + "Ϣ";
		opt = "";
		optional = d.get("resultTime");
		if (optional != null) {
			opt = "" + optional;
		}
		line += opt + "Ϣ";
		
		return line;
		
	}
	
	private String getObservationLine(JSONObject o, JSONObject d, JSONObject f) {
		
		String line = "observationϢ" + o.get("iotId") + "Ϣ";
		line += o.get("phenomenonTime") + "Ϣ";
		line += o.get("result") + "Ϣ";
		line += o.get("resultTime") + "Ϣ";
		
		line += d.get("iotId") + "Ϣ";
		line += f.get("iotId") + "Ϣ";
		
		String opt = "";
		Object optional = o.get("resultQuality");
		if (optional != null) {
			opt = "" + optional;
		}
		line += opt + "Ϣ";
		opt = "";
		optional = o.get("validTime");
		if (optional != null) {
			opt = "" + optional;
		}
		line += opt + "Ϣ";
		opt = "";
		optional = o.get("parameters");
		if (optional != null) {
			JSONObject op = (JSONObject) optional;
			opt = "" + op.toJSONString();
		}
		line += opt + "Ϣ";
		
		return line;
		
	}
	
}
