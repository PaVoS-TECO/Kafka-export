package edu.teco.pavos.pke;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.prefs.Preferences;

/**
 * Verifies for the State of a Download.
 * @author Jean Baumgarten
 */
public class DownloadState {

	protected String dirPath;
	protected String infoPath;
    protected String downloadID;
    protected File filePath;
    protected String ready;
    protected String save = "pavos-export-files";

    /**
     * Default constructor
     * @param id of the Download
     */
    public DownloadState(String id) {
    	
    	this.downloadID = id;
    	File status = new File("/usr/pke/status/" + id + ".txt");
    	boolean exists = status.exists();
    	
    	if (exists) {
    		
			try {
				
				BufferedReader br = new BufferedReader(new FileReader(status.getAbsolutePath()));
				String line;
				
				while ((line = br.readLine()) != null) {
					
					String[] p = line.split(" ");
					if (p.length >= 2 && p[0].equals("ready")) {
						this.ready = p[1];
					} else if (p.length >= 2 && p[0].equals("path")) {
						this.filePath = new File(p[1]);
					}
					
				}
				
				br.close();
			} catch (FileNotFoundException e) {
				this.ready = "error";
	    		this.filePath = new File("/usr/pke/export.properties");
			} catch (IOException e) {
				this.ready = "error";
	    		this.filePath = new File("/usr/pke/export.properties");
			}
    		
    	} else {
    		
    		this.ready = "noID";
    		this.filePath = new File("/usr/pke/export.properties");
    		
    	}
    	
    	/*Preferences prefs = Preferences.userRoot().node(this.save);
    	boolean exists = prefs.getBoolean(this.downloadID, false);
    	
    	if (exists) {
    		
    		String path = prefs.get(this.downloadID + "/Path", "");
    		String ready = prefs.get(this.downloadID + "/Ready", "false");
    		this.filePath = new File(path);
    		this.ready = ready;
    		
    	} else {
    		
    		this.ready = "noID";
    		this.filePath = new File("/usr/pke/export.properties");
    		
    	}*/
    	
    }
    
    /**
     * Gives the String ID associated with this DownloadID.
     * @return The String ID of the File for the Download.
     */
    public String getID() {
    	
    	return this.downloadID;
    	
    }

    /**
     * Gives the FilePath associated with this DownloadID.
     * @return The FilePath of the File for the Download.
     */
    public File getFilePath() {
    	
        return this.filePath;
        
    }

    /**
     * Checks if a File is Ready to be downloaded.
     * @return A boolean whether the file is downloadable or not.
     */
    public String getDownloadState() {
    	
        return this.ready;
        
    }

}
