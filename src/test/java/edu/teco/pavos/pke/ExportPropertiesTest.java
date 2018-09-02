package edu.teco.pavos.pke;

import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

/**
 * Test Case for ExportProperties
 * @author Jean Baumgarten
 */
public class ExportPropertiesTest {

	@Test
	public void test() {

		ExportProperties props = new ExportProperties("csv",
				"2018-07-01T01:01:01Z,2018-08-30T01:01:01Z", "obs,props", "c,id,s");
		TimeIntervall i = props.getTimeFrame();
		DateTime t = new DateTime(2018, 7, 1, 1, 1, 1, DateTimeZone.UTC);
		Set<String> obs = props.getObservedProperties();
		Set<String> clu = props.getClusters();
		
		assertTrue(props.getFileExtension().equals("csv"));
		assertTrue(i.getStartDate().isBefore(i.getEndDate()));
		//assertTrue(i.getStartDate().equals(t));
		assertTrue(obs.contains("obs") && obs.contains("props"));
		assertTrue(clu.contains("c") && clu.contains("id") && clu.contains("s"));
		
	}

}
