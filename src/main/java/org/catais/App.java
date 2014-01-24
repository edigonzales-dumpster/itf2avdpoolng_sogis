package org.catais;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.catais.avwms.Avwms;
import org.catais.exportdata.ExportData;
import org.catais.fusion.Fusion;
import org.catais.geobau.Geobau;
import org.catais.ili2ch.Convert;
import org.catais.ili2freeframe.Transform;
import org.catais.importdata.ImportData;
import org.catais.maintenance.Reindex;
import org.catais.maintenance.Vacuum;
import org.catais.mopublic.ExportMopublic;
import org.catais.svn.Commit2Svn;
import org.catais.utils.DeleteFiles;
import org.catais.utils.IOUtils;
import org.catais.utils.ReadProperties;
import org.postgresql.util.PSQLException;

import ch.interlis.ili2c.Ili2cException;
import ch.interlis.iox.IoxException;


public class App 
{
	private static Logger logger = Logger.getLogger(App.class);
	
    public static void main( String[] args )
    {    	
    	logger.setLevel(Level.DEBUG);

    	String propFileName = null;
    	ArrayList errorFileList = new ArrayList();
    	
		try {
	    	// Read log4j properties file
	    	File tempDir = IOUtils.createTempDirectory("itf2avdpoolng");
			InputStream is =  App.class.getResourceAsStream("log4j.properties");
			File log4j = new File(tempDir, "log4j.properties");
			IOUtils.copy(is, log4j);
	
			// Configure log4j with properties file
			PropertyConfigurator.configure(log4j.getAbsolutePath());
	
			// Begin logging
			logger.info("Start: "+ new Date());
		
			// Get the properties file with all the things we need to know
			propFileName = (String) args[0];
			logger.debug("Properties filename: " + propFileName);
			
			// Read all the properties into a map.
			ReadProperties readproperties = new ReadProperties(propFileName);
			HashMap params = readproperties.read();
			logger.debug(params);
			
			boolean doImport = (Boolean) params.get("doImport");
			String doAvwms = (String) params.get("doAvwms");
			boolean doIli2ch = (Boolean) params.get("doIli2ch");
			boolean doIli2freeframe = (Boolean) params.get("doIli2freeframe");
			boolean doGeobau = (Boolean) params.get("doGeobau");
			boolean doMopublic = (Boolean) params.get("doMopublic");
			boolean doCommit2Svn = (Boolean) params.get("doCommit2Svn");
			boolean doDeleteFiles = (Boolean) params.get("doDeleteFiles");
			String doVacuum = (String) params.get("vacuum");
			String doReindex = (String) params.get("reindex");
			boolean doExport = (Boolean) params.get("doExport");
			boolean doFusion = (Boolean) params.get("doFusion");
			
			logger.info("doImport: " + doImport);
			logger.info("doAvwms: " + doAvwms);
			logger.info("doIli2ch: " + doIli2ch);		
			logger.info("doIli2freeframe: " + doIli2freeframe);	
			logger.info("doGeobau: " + doGeobau);
			logger.info("doMopublic: " + doMopublic);
			logger.info("doCommit2Svn: " + doCommit2Svn);
			logger.info("doDeleteFiles: " + doDeleteFiles);
			logger.info("doVacuum: " + doVacuum);
			logger.info("doReindex: " + doReindex);
			logger.info("doExport: " + doExport);
			logger.info("doFusion: " + doFusion);

			
			// Do the action:
			// Import
			if (doImport == true) {
				logger.info("Start import...");	
				try {
					ImportData importData = new ImportData(params);
					errorFileList = importData.run();
					logger.info("Filelist with errors: " + errorFileList.toString());
					logger.info("End import.");
					
					if (doFusion == true) {
						logger.info("Start fusion...");
						Fusion fusion = new Fusion(params, "so");
						fusion.run();
						logger.info("End fusion.");
					}
					
				} catch (NullPointerException npe) {
					npe.printStackTrace();
					logger.error("NullPointException.");
					logger.error(npe.getMessage());
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
			}
			
			// AV-WMS
			if (doAvwms != null) {
				logger.info("Start AV-WMS...");
				try {
					Avwms avwms = new Avwms(params);
					avwms.run();
				} catch (ClassNotFoundException cnfe) {
					logger.error(cnfe.getMessage());	
				} catch (SQLException sqle) {
					logger.error(sqle.getMessage());
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
				logger.info("End AV-WMS.");
			}
			
			// Vacuum
			if (doVacuum != null) {
				logger.info("Start Vacuum...");
				try {
					Vacuum vacuum = new Vacuum(params);
					vacuum.run();
				} catch (ClassNotFoundException cnfe) {
					logger.error(cnfe.getMessage());	
				} catch (SQLException sqle) {
					logger.error(sqle.getMessage());
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
				logger.info("End Vacuum.");
			}

			// Reindex
			if (doReindex != null) {
				logger.info("Start Reindexing...");
				try {
					Reindex reindex = new Reindex(params);
					reindex.run();
				} catch (ClassNotFoundException cnfe) {
					logger.error(cnfe.getMessage());	
				} catch (SQLException sqle) {
					logger.error(sqle.getMessage());
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
				logger.info("End Reindexing.");
			}
			
			// Ili2ch
			if (doIli2ch == true) {
				logger.info("Start Converting...");
				try {
					Convert convertItf = new Convert(params);
					convertItf.run();
					logger.info("End Converting.");
					
					if (doFusion == true) {
						logger.info("Start fusion...");
						Fusion fusion = new Fusion(params, "ch");
						fusion.run();
						logger.info("End fusion.");
					}					
					
				} catch (Exception e) {
					logger.error(e.getMessage());
					e.printStackTrace();
				}
			}
			
			// Ili2freeframe
			if (doIli2freeframe == true) {
				logger.info("Start Transforming...");
				try {
					Transform transformItf = new Transform(params);
					transformItf.run();
					logger.info("End Transforming.");
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
			}
			
			// Geobau
			if (doGeobau == true) {
				logger.info("Start Geobau...");
				try {
					Geobau geobau = new Geobau(params);
					geobau.run();
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
				logger.info("End Geobau.");
			}
			
			// Mopublic
			if (doMopublic == true) {
				logger.info("Start Mopublic...");
				try {
					ExportMopublic exportMopublic = new ExportMopublic(params);
					exportMopublic.run();
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
				logger.info("End Mopublic.");
			}
			
			// Commit to svn
			if (doCommit2Svn == true) {
				logger.info("Start commiting to svn...");
				try {
					Commit2Svn commit2svn = new Commit2Svn(params);
					commit2svn.run();
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
				logger.info("End commiting files.");
			}
			
			// Delete files
			if (doDeleteFiles == true) {
				logger.info("Start deleting files...");
				try {
					DeleteFiles deleteFiles = new DeleteFiles(params);
					deleteFiles.run(errorFileList);
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
				logger.info("End deleting files.");
			}
			
			// Export data
			if (doExport == true) {
				logger.info("Start exporting data....");
				try {
					ExportData exportData = new ExportData(params);
					exportData.run();
				} catch (IllegalArgumentException e) {
					logger.error(e.getMessage());
				} catch (Ili2cException e) {
					logger.error(e.getMessage());
				} catch (IoxException e) {
					logger.error(e.getMessage());
				} catch (IOException e) {
					logger.error(e.getMessage());
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
				logger.info("End exporting data....");
			}
			
			


		} catch (NullPointerException npe) {
			logger.fatal("NullPointException.");
			logger.fatal(npe.getMessage());
		} catch (IllegalArgumentException iae) {
			logger.fatal(iae.getMessage());
			iae.printStackTrace();
		} catch (FileNotFoundException fnfe) {
			logger.fatal(fnfe.getMessage());
			fnfe.printStackTrace();
		} catch (IOException ioe) {
			logger.fatal(ioe.getMessage());
			ioe.printStackTrace();
		} catch (ArrayIndexOutOfBoundsException aioobe) {
			logger.fatal(aioobe.getMessage());
			aioobe.printStackTrace();
		} finally {
			// Stop logging
        	logger.info("End: "+ new Date());
		}
		
		


        
//        // Get all the actions we have to do from the properties file
//		try {
//			Properties properties = new Properties();
//			BufferedInputStream stream = new BufferedInputStream(new FileInputStream(propFileName));
//			properties.load(stream);
//			stream.close();
//		} catch (FileNotFoundException fnfe) {
//        	logger.fatal("Properties file not found: " + propFileName);
//        	logger.info("Ende2: "+ new Date());
//			throw new FileNotFoundException();
//		}
    }
}
