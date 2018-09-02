package edu.teco.pavos.pke;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Test Case for FileTypeUtility
 * @author Jean Baumgarten
 */
public class FileTypesUtilityTest {

	@Test
	public void test() {
		
		ExportProperties props = new ExportProperties("csv",
				"2018-07-01T01:01:01Z,2018-08-30T01:01:01Z", "obs,props", "c,id,s");
		
		try {
			FileWriterStrategy writer = FileTypesUtility.getFileWriterForFileExtension(props);
			assertTrue(true);
		} catch (IllegalFileExtensionException e) {
			assertTrue(false);
		}
		
		props = new ExportProperties("pdf",
				"2018-07-01T01:01:01Z,2018-08-30T01:01:01Z", "obs,props", "c,id,s");
		
		try {
			FileWriterStrategy writer = FileTypesUtility.getFileWriterForFileExtension(props);
			assertTrue(false);
		} catch (IllegalFileExtensionException e) {
			assertTrue(true);
		}
		
	}

}
