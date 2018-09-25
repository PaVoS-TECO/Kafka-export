package edu.teco.pavos.pke;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Test;

import com.opencsv.CSVWriter;

/**
 * Test case for CSVWriterStrategy
 * @author Jean Baumgarten
 */
public class CSVWriterStrategyTest {

	@Test
	public void testClusters() {

		ExportProperties props = new ExportProperties("csv",
				"2018-07-01T01:01:01Z,2018-08-30T01:01:01Z", "temperature", "cID:1_0-0_1,cID:1_0-1_1");
		
		CSVWriterStrategy writer = new CSVWriterStrategy(props);
		String data1 = "cID:1_0-0_1-1_0";
		String data2 = "cID:1_0-1_1";
		String data3 = "cID:1_0-1_0";
		
		Method method;
		try {
			
			method = CSVWriterStrategy.class.getDeclaredMethod("isOneOfOrContainedInExportClusters", String.class);
			method.setAccessible(true);
			boolean bool1 = (boolean) method.invoke(writer, new Object[] { data1 });
			boolean bool2 = (boolean) method.invoke(writer, new Object[] { data2 });
			boolean bool3 = (boolean) method.invoke(writer, new Object[] { data3 });

			assertTrue(bool1);
			assertTrue(bool2);
			assertTrue(!bool3);
			
		} catch (NoSuchMethodException e) {
			System.out.println(e.getLocalizedMessage());
			assertTrue(false);
		} catch (SecurityException e) {
			System.out.println(e.getLocalizedMessage());
			assertTrue(false);
		} catch (IllegalAccessException e) {
			System.out.println(e.getLocalizedMessage());
			assertTrue(false);
		} catch (IllegalArgumentException e) {
			System.out.println(e.getLocalizedMessage());
			assertTrue(false);
		} catch (InvocationTargetException e) {
			System.out.println(e.getLocalizedMessage());
			assertTrue(false);
		}
		
	}
	
	@Test
	public void test1() {
		
		JSONObject op = new JSONObject();
		op.put("iotId", "ID");
		op.put("name", "OP");
		op.put("description", "Desc");
		op.put("definition", "Deff");
		
		String[] out = {
				"observedProperty",
				(String) op.get("iotId"),
				(String) op.get("name"),
				(String) op.get("description"),
				(String) op.get("definition")
			};
		
		PrintWriter writer;
		try {
			writer = new PrintWriter(System.getProperty("user.home") + File.separator + "Desktop/newTest.csv", "UTF-8");
			CSVWriter csvWriter = new CSVWriter(writer,
	                CSVWriter.DEFAULT_SEPARATOR,
	                CSVWriter.DEFAULT_QUOTE_CHARACTER,
	                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
	                CSVWriter.DEFAULT_LINE_END);
			csvWriter.writeNext(out);
			csvWriter.writeNext(out);
			csvWriter.close();
			writer.close();
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

	@Test
	public void testGetLines() {

		ExportProperties props = new ExportProperties("csv",
				"2018-07-01T01:01:01Z,2018-08-30T01:01:01Z", "temperature", "cID:1_0-0_1,cID:1_0-1_1");
		
		CSVWriterStrategy writer = new CSVWriterStrategy(props);
		JSONParser parser = new JSONParser();
		String data;
		
		Method method;
		try {
			
			data = "{\"iotId\":\"1\",\"name\":\"TMS\",\"description\":\"Sensor\",\"properties\":{\"DC\":\"Deployed\",\"CU\":\"Radiation shield\"}}";
			JSONObject data1 = (JSONObject) parser.parse(data);
			data = "{\"iotId\":\"1\",\"name\":\"UoC\",\"description\":\"UofC\",\"encodingType\":\"application/vnd.geo+json\",\"location\":{\"type\":\"Point\",\"coordinates\":[-114.133,51.08]}}";
			JSONObject data2 = (JSONObject) parser.parse(data);
			data = "{\"iotId\":\"1\",\"name\":\"D\",\"description\":\"D\",\"encodingType\":\"application/pdf\",\"metadata\":\"https\"}";
			JSONObject data3 = (JSONObject) parser.parse(data);
			data = "{\"iotId\":\"1\",\"name\":\"AT\",\"description\":\"The\",\"definition\":\"http\"}";
			JSONObject data4 = (JSONObject) parser.parse(data);
			data = "{\"iotId\":\"1\",\"name\":\"UofC\",\"description\":\"UofC\",\"encodingType\":\"application/vnd.geo+json\",\"feature\":{\"type\":\"Point\",\"coordinates\":[-114.133,51.08]}}";
			JSONObject data5 = (JSONObject) parser.parse(data);
			
			method = CSVWriterStrategy.class.getDeclaredMethod("getThingLine", JSONObject.class);
			method.setAccessible(true);
			String line = (String) method.invoke(writer, new Object[] { data1 });
			assertTrue(line.equals("thingϢ1ϢTMSϢSensorϢ{\"CU\":\"Radiation shield\",\"DC\":\"Deployed\"}"));
			
			method = CSVWriterStrategy.class.getDeclaredMethod("getLocationLine", JSONObject.class);
			method.setAccessible(true);
			line = (String) method.invoke(writer, new Object[] { data2 });
			assertTrue(line.equals("locationϢ1ϢUoCϢUofCϢapplication/vnd.geo+jsonϢ{\"coordinates\":[-114.133,51.08],\"type\":\"Point\"}"));
			
			method = CSVWriterStrategy.class.getDeclaredMethod("getSensorLine", JSONObject.class);
			method.setAccessible(true);
			line = (String) method.invoke(writer, new Object[] { data3 });
			assertTrue(line.equals("sensorϢ1ϢDϢDϢapplication/pdfϢhttps"));
			
			method = CSVWriterStrategy.class.getDeclaredMethod("getObservedPropertyLine", JSONObject.class);
			method.setAccessible(true);
			line = (String) method.invoke(writer, new Object[] { data4 });
			assertTrue(line.equals("observedPropertyϢ1ϢATϢTheϢhttp"));
			
			method = CSVWriterStrategy.class.getDeclaredMethod("getFeatureLine", JSONObject.class);
			method.setAccessible(true);
			line = (String) method.invoke(writer, new Object[] { data5 });
			assertTrue(line.equals("featureOfInterestϢ1ϢUofCϢUofCϢapplication/vnd.geo+jsonϢ{\"coordinates\":[-114.133,51.08],\"type\":\"Point\"}"));
			
		} catch (NoSuchMethodException e) {
			System.out.println(e.getLocalizedMessage());
			System.out.println("NoSuchMethodException");
			assertTrue(false);
		} catch (SecurityException e) {
			System.out.println(e.getLocalizedMessage());
			System.out.println("SecurityException");
			assertTrue(false);
		} catch (IllegalAccessException e) {
			System.out.println(e.getLocalizedMessage());
			System.out.println("IllegalAccessException");
			assertTrue(false);
		} catch (IllegalArgumentException e) {
			System.out.println(e.getLocalizedMessage());
			System.out.println("IllegalArgumentException");
			assertTrue(false);
		} catch (InvocationTargetException e) {
			System.out.println(e.getLocalizedMessage());
			System.out.println("InvocationTargetException");
			assertTrue(false);
		} catch (ParseException e) {
			System.out.println(e.getLocalizedMessage());
			System.out.println("ParseException");
			assertTrue(false);
		}
		
	}

}
