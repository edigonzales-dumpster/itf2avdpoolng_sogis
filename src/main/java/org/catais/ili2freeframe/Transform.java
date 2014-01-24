package org.catais.ili2freeframe;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.nio.channels.Channels;
import java.util.HashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.catais.ili2ch.Convert;
import org.catais.ili2ch.Ili2ch;

import ch.interlis.ili2c.Ili2cException;

public class Transform {
	private static Logger logger = Logger.getLogger(Transform.class);
	
	private HashMap params = null;
	private String inputRepoModelName = null;
	private String outputRepoModelName = null;
	private String ili2freeframeDir = null;
	private String importSourceDir = null;	
	private String ili2chDir = null;


	public Transform(HashMap params) {
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
    			logger.info("Ili2freeframe: " + dir.getAbsolutePath() + dir.separator + f);    			
    			try {
        			Ili2FreeFrame ili2freeframe = new Ili2FreeFrame();
        			String outputFileName = ili2freeframeDir + dir.separator + "ch_lv95_" + f;
        			ili2freeframe.transform(inputRepoModelName, outputRepoModelName, new File(ili2chDir).getAbsolutePath() + dir.separator + "ch_" + f, outputFileName);
	    			logger.info("Transformed: " + dir.getAbsolutePath() + dir.separator + "ch_" + f);
	    			
    				logger.info("Zipping file...");
		        	File destinationFile = new File(outputFileName);    		        	
        			String outputZipFileName = ili2freeframeDir + dir.separator + "ch_lv95_" + f.substring(0, 6) + ".zip";
        			
    				FileOutputStream zipfile = new FileOutputStream(outputZipFileName);
    				ZipOutputStream zipOutputStream = new ZipOutputStream(zipfile);
    				ZipEntry zipEntry = new ZipEntry(destinationFile.getName());
    				zipOutputStream.putNextEntry(zipEntry);
    				new FileInputStream(destinationFile).getChannel().transferTo(0, destinationFile.length(), Channels.newChannel(zipOutputStream));
    				zipOutputStream.closeEntry();
    				zipOutputStream.close();
	    			logger.info("File zipped: " + outputZipFileName);

				} catch (Ili2cException e) {
					logger.error(e.getMessage());
				} catch (Exception e) {
					logger.error(e.getMessage());
					e.printStackTrace();
				}
    		}
    	}
	}
	
	private void readParams() {		
    	this.inputRepoModelName = (String) params.get("ili2chModelName");
		logger.debug("inputRepoModelName: " + this.inputRepoModelName);
		if (this.inputRepoModelName == null) {
			throw new IllegalArgumentException("importModelName not set.");
		}	
		
    	this.outputRepoModelName = (String) params.get("ili2freeframeModelName");
		logger.debug("outputRepoModelName: " + this.outputRepoModelName);
		if (this.outputRepoModelName == null) {
			throw new IllegalArgumentException("ili2freeframeModelName not set.");
		}	
		
    	this.ili2freeframeDir = (String) params.get("ili2freeframeDir");
		logger.debug("ili2freeframeDir: " + this.ili2freeframeDir);		
		if (this.ili2freeframeDir == null) {
			throw new IllegalArgumentException("ili2freeframeDir not set.");
		}	
	
		this.importSourceDir = (String) params.get("importSourceDir");
		logger.debug("Import source Directory: " + this.importSourceDir);
		if (importSourceDir == null) {
			throw new IllegalArgumentException("Import source dir not set.");
		}
		
		this.ili2chDir = (String) params.get("ili2chDir");
		logger.debug("ili2chDir source Directory: " + this.ili2chDir);
		if (ili2chDir == null) {
			throw new IllegalArgumentException("ili2chDir source dir not set.");
		}   
    }
}
