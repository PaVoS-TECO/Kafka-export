package edu.teco.pavos.pke;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Test Case for FileType
 * @author Jean Baumgarten
 */
public class FileTypeTest {

	@Test
	public void test() {

		ExportProperties props = new ExportProperties("csv",
				"2018-07-01T01:01:01Z,2018-08-30T01:01:01Z", "obs,props", "c,id,s");
		FileType type = new FileType(props);
		
		try {
			FileWriterStrategy writer = type.getFileWriter();
			assertTrue(true);
		} catch (IllegalFileExtensionException e) {
			assertTrue(false);
		}
		
		props = new ExportProperties("pdf",
				"2018-07-01T01:01:01Z,2018-08-30T01:01:01Z", "obs,props", "c,id,s");
		type = new FileType(props);
		
		try {
			FileWriterStrategy writer = type.getFileWriter();
			assertTrue(false);
		} catch (IllegalFileExtensionException e) {
			assertTrue(true);
		}
		
	}

}
