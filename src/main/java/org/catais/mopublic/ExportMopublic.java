package org.catais.mopublic;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.catais.ili2freeframe.Ili2FreeFrame;
import org.catais.ili2freeframe.Transform;

import ch.interlis.ili2c.Ili2cException;

public class ExportMopublic {
	
	private static Logger logger = Logger.getLogger(ExportMopublic.class);
	
	private HashMap params = null;
	private String mopublicBaseDir = null;
	private String importSourceDir = null;


	public ExportMopublic(HashMap params) {
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
    			logger.info("MOpublic: " + dir.getAbsolutePath() + dir.separator + f);    			
    			try {
					Integer gem_bfs = Integer.valueOf(f.substring(0, 4));
					logger.debug(gem_bfs);

					String[] topics = {"Control_points", "Land_cover", "Single_objects", "Heights", "Local_names", "Ownership", "Pipelines", "Territorial_boundaries", "Building_addresses"};

    				MOpublic mopublic = new MOpublic(params);
//					mopublic.write(f.substring(0, 4), "d", topics, "sqlite", "LV03");

//    				mopublic = new MOpublic(params);
					mopublic.write(f.substring(0, 4), "d", topics, "shp", "LV03");

//    				mopublic = new MOpublic(params);
//					mopublic.write(f.substring(0, 4), "e", topics, "shp", "LV03");
//
//    				mopublic = new MOpublic(params);
//					mopublic.write(f.substring(0, 4), "d", topics, "shp", "LV95");
		
    				logger.info("MOpublic created: " + f);
				} catch (ClassNotFoundException e) {
					logger.error("Class not found.");
					logger.error(e.getMessage());
				} catch (NumberFormatException nfe) {
    				logger.error(nfe.getMessage());
    			} catch (Exception e) {
					logger.error(e.getMessage());
				}
    		}
    		if (itfFileList.length > 0) {
				try {
					logger.debug("MOpublic ganzer Kanton");
	
					String[] topics = {"Control_points", "Land_cover", "Single_objects", "Heights", "Local_names", "Ownership", "Pipelines", "Territorial_boundaries", "Building_addresses"};
					MOpublic mopublic = new MOpublic(params);
//					mopublic.write("kanton", "d", topics, "shp", "LV03");
					
					logger.info("MOpublic Kanton created.");
				} catch (ClassNotFoundException e) {
					logger.error("Class not found.");
					logger.error(e.getMessage());
				} catch (NumberFormatException nfe) {
					logger.error(nfe.getMessage());
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
    		}
    	}
	}
	
	
	private void readParams() {		
    	this.mopublicBaseDir = (String) params.get("mopublicBaseDir");
		logger.debug("mopublicBaseDir: " + this.mopublicBaseDir);		
		if (this.mopublicBaseDir == null) {
			throw new IllegalArgumentException("mopublicBaseDir not set.");
		}	
	
		this.importSourceDir = (String) params.get("importSourceDir");
		logger.debug("Import source Directory: " + this.importSourceDir);
		if (importSourceDir == null) {
			throw new IllegalArgumentException("Import source dir not set.");
		}    	
    }
}
