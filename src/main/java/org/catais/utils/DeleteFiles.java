package org.catais.utils;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.catais.importdata.ImportData;

public class DeleteFiles {
	
	private static Logger logger = Logger.getLogger(DeleteFiles.class);
	
	private HashMap params = null;
	private String importSourceDir = null;


	public DeleteFiles(HashMap params) {
		logger.setLevel(Level.INFO);
		
		this.params = params;
		readParams();
	}
	
	
	public void run(ArrayList errorFileList) throws Exception {
    	File dir = new File(importSourceDir);
    	String[] itfFileList = dir.list(new FilenameFilter() {
    		public boolean accept(File d, String name) {
    			return name.toLowerCase().endsWith(".itf"); 
    		}
    	});
    	logger.debug("Count of itf files: " + itfFileList.length);  	
    	logger.info("Files NOT to delete: " + errorFileList.toString());
    	
    	if (itfFileList != null) {
    		for(String f : itfFileList) {
    			logger.info("Delete: " + dir.getAbsolutePath() + dir.separator + f);
    			
    			if (errorFileList.contains(f) == true) {
    				logger.info("Itf will not be deleted: " + f);
    				continue;
    			} else {
        			File sourceFile = new File(dir.getAbsolutePath() + dir.separator + f);
    				boolean checkDel = sourceFile.delete();
    				if (checkDel == true) {
    					logger.info("File deleted: " + sourceFile.getAbsolutePath());
    				} else {
    					logger.error("Could not delete file: " + sourceFile.getAbsolutePath());
    				}
    			}
    		}
    	}
	}
	
	
	private void readParams() {		
		importSourceDir = (String) params.get("importSourceDir");
		logger.debug("Import Source Directory: " + importSourceDir);
		if (importSourceDir == null) {
			throw new IllegalArgumentException("Import source dir not set.");
		}	
	}


}
