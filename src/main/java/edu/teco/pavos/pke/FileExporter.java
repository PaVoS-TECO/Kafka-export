package edu.teco.pavos.pke;

import java.io.File;

/**
 * Exporter of Data from Kafka to a File.
 * @author Jean Baumgarten
 */
public class FileExporter {
	
	private ExportProperties properties;
	private AlterableDownloadState ads;

    /**
     * Default constructor
     * @param properties for the export
     * @param downloadID of the download
     */
    public FileExporter(ExportProperties properties, String downloadID) {
    	
    	this.properties = properties;
    	this.ads =  new AlterableDownloadState(downloadID);
    	this.ads.setFilePreparingForDownload();
    	this.ads.savePersistent();
    	
    }

    /**
     * Generates the File with the desired Data.
     */
    public void createFile() {
    	
    	String extension = this.properties.getFileExtension();
    	
    	try {
    		
			FileWriterStrategy fileWriter = FileTypesUtility.getFileWriterForFileExtension(this.properties);
			String filename = this.ads.getID() + "." + extension;
			String dirPath = "/usr/pke/exports";
	    	String path = dirPath + File.separator + filename;
	    	
	    	File directory = new File(dirPath);
	    	if (!directory.exists()) {
	    		directory.mkdir();
	    	}
	    	
	    	fileWriter.saveToFile(new File(path));
	    	this.ads.setFilePath(new File(path));
	    	this.ads.setFileReadyForDownload();
	    	this.ads.savePersistent();
	    	
		} catch (IllegalFileExtensionException e) {
			
			this.ads.setFileHadError();
			this.ads.savePersistent();
			
		}
    	
    }

}
