package edu.teco.pavos.pke;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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

import com.opencsv.CSVWriter;

/**
 * Implementation of the FileWriterStrategy interface for CSV files.
 * @author Jean Baumgarten
 */
public class CSVWriterStrategy implements FileWriterStrategy {

    private HashSet<String> features = new HashSet<String>();
    private HashSet<String> dataStreams = new HashSet<String>();
    //private HashSet<String> locations = new HashSet<String>();
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
	private CSVWriter csvWriter;

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
			
			this.csvWriter = new CSVWriter(this.writer,
                    CSVWriter.DEFAULT_SEPARATOR,
                    CSVWriter.DEFAULT_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END);
			
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
			this.csvWriter.close();
			this.writer.close();
			
		} catch (FileNotFoundException e) {
			
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		} catch (UnsupportedEncodingException e) {
			
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		} catch (IOException e) {
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
							this.csvWriter.writeNext(this.getObservedPropertyLine(observedProperty));
							//this.writer.println(this.getObservedPropertyLine(observedProperty));
						
						if (!this.sensors.contains((String) sensor.get("iotId")))
							this.csvWriter.writeNext(this.getSensorLine(sensor));
							//this.writer.println(this.getSensorLine(sensor));
						
						if (!this.things.contains((String) thing.get("iotId")))
							this.csvWriter.writeNext(this.getThingLine(thing));
							//this.writer.println(this.getThingLine(thing));
						
						if (!this.features.contains((String) featureOfInterest.get("iotId")))
							this.csvWriter.writeNext(this.getFeatureLine(featureOfInterest));
							//this.writer.println(this.getFeatureLine(featureOfInterest));
						
						if (!this.dataStreams.contains((String) dataStream.get("iotId")))
							this.csvWriter.writeNext(this.getDataStreamLine(dataStream, thing, observedProperty, sensor));
							//this.writer.println(this.getDataStreamLine(dataStream, thing, observedProperty, sensor));
						
						this.csvWriter.writeNext(this.getObservationLine(obs, dataStream, featureOfInterest));
						//this.writer.println(this.getObservationLine(obs, dataStream, featureOfInterest));
						
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
	
	private String[] getObservedPropertyLine(JSONObject op) {
		
		this.observedProperties.add(op.get("iotId").toString());
		
		String[] out = {
				"observedProperty",
				(String) op.get("iotId"),
				(String) op.get("name"),
				(String) op.get("description"),
				(String) op.get("definition")
			};
		
		return out;
		
	}
	
	private String[] getSensorLine(JSONObject s) {
		
		this.sensors.add(s.get("iotId").toString());
		
		String[] out = {
				"sensor",
				(String) s.get("iotId"),
				(String) s.get("name"),
				(String) s.get("description"),
				(String) s.get("encodingType"),
				(String) s.get("metadata")
			};
		
		return out;
		
	}
	
	private String[] getFeatureLine(JSONObject f) {
		
		this.features.add(f.get("iotId").toString());
		
		JSONObject feature = (JSONObject) f.get("feature");
		String coords = "[" + feature.get("coordinates") + "]";
		feature.put("coordinates", coords);
		
		String[] out = {
				"featureOfInterest",
				(String) f.get("iotId"),
				(String) f.get("name"),
				(String) f.get("description"),
				(String) f.get("encodingType"),
				toJsonString(feature)
			};
		
		return out;
		
	}
	
	private String[] getThingLine(JSONObject t) {
		
		this.things.add(t.get("iotId").toString());
		
		String opt = "";
		Object optional = t.get("properties");
		if (optional != null) {
			opt = toJsonString((JSONObject) optional);
		}
		
		String[] out = {
				"thing",
				(String) t.get("iotId"),
				(String) t.get("name"),
				(String) t.get("description"),
				opt
			};
		
		return out;
		
	}
	
	private String[] getDataStreamLine(JSONObject d, JSONObject t, JSONObject op, JSONObject s) {
		
		this.dataStreams.add(d.get("iotId").toString());
		
		String optOA = "";
		Object optional = d.get("observedArea");
		if (optional != null) {
			optOA = toJsonString((JSONObject) optional);
		}
		
		String optPT = "";
		optional = d.get("phenomenonTime");
		if (optional != null) {
			optPT = "" + optional;
		}
		
		String optRT = "";
		optional = d.get("resultTime");
		if (optional != null) {
			optRT = "" + optional;
		}
		
		String[] out = {
				"dataStream",
				(String) d.get("iotId"),
				(String) d.get("name"),
				(String) d.get("description"),
				(String) d.get("observationType"),
				toJsonString((JSONObject) d.get("unitOfMeasurement")),
				(String) t.get("iotId"),
				(String) op.get("iotId"),
				(String) s.get("iotId"),
				optOA,
				optPT,
				optRT
			};
		
		return out;
		
	}
	
	private String[] getObservationLine(JSONObject o, JSONObject d, JSONObject f) {
		
		String optRQ = "";
		Object optional = o.get("resultQuality");
		if (optional != null) {
			optRQ = "" + optional;
		}
		
		String optVT = "";
		optional = o.get("validTime");
		if (optional != null) {
			optVT = "" + optional;
		}
		
		String optP = "";
		optional = o.get("parameters");
		if (optional != null) {
			JSONObject op = (JSONObject) optional;
			optP = "" + toJsonString(op);
		}
		
		String[] out = {
				"observation",
				(String) o.get("iotId"),
				(String) o.get("phenomenonTime"),
				(String) o.get("result"),
				(String) o.get("resultTime"),
				(String) d.get("iotId"),
				(String) f.get("iotId"),
				optRQ,
				optVT,
				optP
			};
		
		return out;
		
	}
	
	private static String toJsonString(JSONObject object) {
		
		String json = "{";
		
		for (Object oKey : object.keySet()) {
			String key = "" + oKey;
			String value = "" + object.get(key);
			json += "\"" + key + "\":";
			if (value.startsWith("[")) {
				json += value + ",";
			} else {
				json += "\"" + value + "\",";
			}
		}
		
        json = json.substring(0, json.length() - 1);		
        json += "}";
        
		return json;
	}
	
	/*
	
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
		
	}*/
	
}
