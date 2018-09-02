package edu.teco.pavos.pke;

import static org.junit.Assert.assertTrue;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

/**
 * Test Case for TimeIntervall
 * @author Jean Baumgarten
 */
public class TimeIntervallTest {

	@Test
	public void test() {

		TimeIntervall i = new TimeIntervall("2018-07-01T01:01:01Z,2018-08-30T01:01:01Z");
		DateTime t = new DateTime(2018, 8, 1, 1, 1, 1, DateTimeZone.UTC);
		
		assertTrue(i.getStartDate().isBefore(i.getEndDate()));
		assertTrue(i.isInside(t));
		
	}

}
