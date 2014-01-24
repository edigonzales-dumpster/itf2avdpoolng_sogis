package org.catais.ili2ch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.catais.importdata.ImportData;
import org.catais.utils.IOUtils;

import ch.interlis.ili2c.Ili2cException;

public class Convert {
	private static Logger logger = Logger.getLogger(Convert.class);
	
	private HashMap params = null;
	private String inputRepoModelName = null;
	private String outputRepoModelName = null;
	private String ili2chDir = null;
	private String importSourceDir = null;

	
	public Convert(HashMap params) {
    	logger.setLevel(Level.INFO);
    	
    	this.params = params;
    	readParams();
	}
	
	
	public void run() throws Exception {
    	File dir = new File(importSourceDir);
    	String[] itfFileList = dir.list(new FilenameFilter() {
    		public boolean accept(File d, String name) {
    			return name.toLowerCase().endsWith(".itf"); 
    		}
    	});
    	logger.debug("Count of itf files: " + itfFileList.length);

    	if (itfFileList != null) {
    		for(String f : itfFileList) {
    			logger.info("Ili2ch: " + dir.getAbsolutePath() + dir.separator + f);    			
    			try {
        			Ili2ch ili2ch = new Ili2ch();
        			String outputFileName = ili2chDir + dir.separator + "ch_" + f;
					ili2ch.convert(inputRepoModelName, outputRepoModelName, dir.getAbsolutePath() + dir.separator + f, outputFileName);
	    			logger.info("Converted: " + dir.getAbsolutePath() + dir.separator + f);
	    			
    				logger.info("Zipping file...");
    				// We add also some metadata files to the zipfile (e.g. ili file).
    		    	File tempDir = IOUtils.createTempDirectory("itf2avdpoolng");
    				InputStream is =  Convert.class.getResourceAsStream("DM.01-AV-CH_LV03_24d_ili1.ili");
    				File iliFile = new File(tempDir, "DM.01-AV-CH_LV03_24d_ili1.ili");
    				IOUtils.copy(is, iliFile);    
    				
    				InputStream isHinweise =  ImportData.class.getResourceAsStream("Hinweise.pdf");
    				File hinweiseFile = new File(tempDir, "Hinweise.pdf");
    				IOUtils.copy(isHinweise, hinweiseFile);
    				
		        	File destinationFile = new File(outputFileName);    		        	
        			String outputZipFileName = ili2chDir + dir.separator + "ch_" + f.substring(0, 6) + ".zip";
        			
    				FileOutputStream zipfile = new FileOutputStream(outputZipFileName);
    				ZipOutputStream zipOutputStream = new ZipOutputStream(zipfile);
    				
    				// ITF
    				ZipEntry itfZipEntry = new ZipEntry(destinationFile.getName());
    				zipOutputStream.putNextEntry(itfZipEntry);
    				new FileInputStream(destinationFile).getChannel().transferTo(0, destinationFile.length(), Channels.newChannel(zipOutputStream));
    				
    				// ILI
    				ZipEntry iliZipEntry = new ZipEntry(iliFile.getName());
    				zipOutputStream.putNextEntry(iliZipEntry);
    				new FileInputStream(iliFile).getChannel().transferTo(0, iliFile.length(), Channels.newChannel(zipOutputStream));
    				
    				// Hinweise
    				ZipEntry hinweiseZipEntry = new ZipEntry(hinweiseFile.getName());
    				zipOutputStream.putNextEntry(hinweiseZipEntry);
    				new FileInputStream(hinweiseFile).getChannel().transferTo(0, hinweiseFile.length(), Channels.newChannel(zipOutputStream));
    				
    				zipOutputStream.closeEntry();
    				zipOutputStream.close();
	    			logger.info("File zipped: " + outputZipFileName);
				} catch (Ili2cException e) {
					logger.error(e.getMessage());
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
    		}
    	}
	}
	
	
	private void readParams() {		
    	this.inputRepoModelName = (String) params.get("importModelName");
		logger.debug("inputRepoModelName: " + this.inputRepoModelName);
		if (this.inputRepoModelName == null) {
			throw new IllegalArgumentException("importModelName not set.");
		}	
		
    	this.outputRepoModelName = (String) params.get("ili2chModelName");
		logger.debug("outputRepoModelName: " + this.outputRepoModelName);
		if (this.outputRepoModelName == null) {
			throw new IllegalArgumentException("ili2chModelName not set.");
		}	
		
    	this.ili2chDir = (String) params.get("ili2chDir");
		logger.debug("ili2chDir: " + this.ili2chDir);		
		if (this.ili2chDir == null) {
			throw new IllegalArgumentException("ili2chDir not set.");
		}	
	
		this.importSourceDir = (String) params.get("importSourceDir");
		logger.debug("Import source Directory: " + this.importSourceDir);
		if (importSourceDir == null) {
			throw new IllegalArgumentException("Import source dir not set.");
		}    
    }
}
