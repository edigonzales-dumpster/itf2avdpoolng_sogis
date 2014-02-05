package org.catais.importdata;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.catais.App;
import org.catais.geobau.GeobauObj;
import org.catais.utils.IOUtils;

import ch.interlis.ili2c.Ili2cException;

public class ImportData {
	private static Logger logger = Logger.getLogger(ImportData.class);
	
	HashMap params = null;
	String inputRepoModelName = null;
	String importSourceDir = null;
	String importDestinationDir = null;
	boolean addAttr = false;
	boolean enumText = false;	
	boolean renumberTid = false;
	
	boolean hasErrors = false;
	ArrayList errorFilesList = new ArrayList();
	
	
	public ImportData(HashMap params) {
		logger.setLevel(Level.INFO);
		
		this.params = params;
		readParams();
	}

	private void readParams() {
    	this.inputRepoModelName = (String) params.get("importModelName");
		logger.debug("inputRepoModelName: " + this.inputRepoModelName);
		if (this.inputRepoModelName == null) {
			throw new IllegalArgumentException("importModelName not set.");
		}	
		
		importSourceDir = (String) params.get("importSourceDir");
		logger.debug("Import Source Directory: " + importSourceDir);
		if (importSourceDir == null) {
			throw new IllegalArgumentException("Import source dir not set.");
		}	
		
		importDestinationDir = (String) params.get("importDestinationDir");
		logger.debug("Import destination Directory: " + importDestinationDir);		
		
		addAttr = (Boolean) params.get("addAttr");
		logger.debug("Additional Attributes: " + addAttr);	
		
		enumText = (Boolean) params.get("enumText");
		logger.debug("Enumeration Text: " + enumText);	
		
		renumberTid = (Boolean) params.get("renumberTid");
		logger.debug("Renumber TID: " + renumberTid);			
		
	}
	
	
	public ArrayList run() throws NullPointerException {
		// Get the list of all itf files we have to import
    	File dir = new File(importSourceDir);
    	String[] itfFileList = dir.list(new FilenameFilter() {
    		public boolean accept(File d, String name) {
    			return name.toLowerCase().endsWith(".itf"); 
    		}
    	});
    	try {
        	logger.debug("Count of itf files: " + itfFileList.length);
    	} catch (Exception e) {
    		logger.debug("No files found.");
    	}

    	if (itfFileList != null) {
    		for(String f : itfFileList) {
    			logger.info("Import: " + dir.getAbsolutePath() + dir.separator + f);

    			LinkedHashMap additionalAttributes = new LinkedHashMap();
    			try {
    			
    				if (addAttr) {        			
    					//additionalAttributes.put("gem_bfs", Integer.valueOf(f.substring(8, 12)).intValue());
    					//additionalAttributes.put("los", Integer.valueOf(f.substring(13, 14)).intValue());
    					//additionalAttributes.put("gem_bfs", Integer.valueOf(f.substring(3, 7)).intValue());
    					//additionalAttributes.put("los", Integer.valueOf(f.substring(8, 9)).intValue());
    					additionalAttributes.put("gem_bfs", Integer.valueOf(f.substring(0, 4)).intValue());
                        additionalAttributes.put("los", Integer.valueOf(f.substring(4, 6)).intValue());
    					additionalAttributes.put("lieferdatum", new Date());  
    				}
    				
    				IliReader reader = new IliReader(additionalAttributes, params);
    				reader.compileModel();
    				//reader.setTidPrefix(f.substring(8, 12) + f.substring(13, 14));
    				//reader.setTidPrefix(f.substring(3, 7) + f.substring(8, 9));
    				reader.setTidPrefix(f.substring(0, 4) + f.substring(4, 6)); 
    				reader.startTransaction();
    				reader.delete();
    				File sourceFile = new File(dir.getAbsolutePath() + dir.separator + f);
    				reader.read(sourceFile.getAbsolutePath(), renumberTid, false);
    				reader.commitTransaction();
    				
    				if (importDestinationDir != null) {
    					logger.info("Copying file...");
    					String outFileName = importDestinationDir.trim() + dir.separator + f;
    		        	File destinationFile = new File(outFileName);    		        	
        				IOUtils.copy(sourceFile, destinationFile);
        				logger.info("File copied: " + destinationFile.getAbsolutePath());
        				
        				logger.info("Zipping file...");
        				// We add also some metadata files to the zipfile (e.g. ili file).
        		    	File tempDir = IOUtils.createTempDirectory("itf2avdpoolng");
        				InputStream is =  ImportData.class.getResourceAsStream("dm01avso24.ili");
        				File iliFile = new File(tempDir, "dm01avso24.ili");
        				IOUtils.copy(is, iliFile);
        				
        				InputStream isHinweise =  ImportData.class.getResourceAsStream("Hinweise.pdf");
        				File hinweiseFile = new File(tempDir, "Hinweise.pdf");
        				IOUtils.copy(isHinweise, hinweiseFile);

        				String outZipFileName = importDestinationDir.trim() + dir.separator + f.substring(0, 6) + ".zip";
        				FileOutputStream zipfile = new FileOutputStream(outZipFileName);
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
        				
        				
        				logger.info("File zipped: " + outZipFileName);
    				}
    				
        			logger.info("Import done: " + f);
    				
    			} catch (Ili2cException ie) {
    				ie.printStackTrace();
    				errorFilesList.add(f);
    				logger.error(ie.getMessage());
    			} catch (FileNotFoundException fnfe) {
    				fnfe.printStackTrace();
    				errorFilesList.add(f);
    				logger.error(fnfe.getMessage());
    			} catch (IOException ioe) {
    				ioe.printStackTrace();
    				errorFilesList.add(f);
    				logger.error(ioe.getMessage());
    			} catch (NumberFormatException nfe) {
    				errorFilesList.add(f);
    				logger.error("Could not get fos number: " + f);
    				logger.error(nfe.getMessage());
    			} catch (IllegalArgumentException iae) {
    				errorFilesList.add(f);
    				logger.error(iae.getMessage());
    			} catch (IllegalStateException ise) {
    				errorFilesList.add(f);
    				logger.error(ise.getMessage());
    			} catch (Exception e) {
    				e.printStackTrace();
    				errorFilesList.add(f);
    				logger.error(e.getMessage());
    			}
    		}
    	}
    	return errorFilesList;
	}
	

}
