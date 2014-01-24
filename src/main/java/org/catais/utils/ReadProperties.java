package org.catais.utils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ReadProperties {
	private static Logger logger = Logger.getLogger(ReadProperties.class);
	
	String fileName = null;
	
    public ReadProperties(String fileName) {
		logger.setLevel(Level.INFO);
		
    	this.fileName = fileName;
    }
    
    
    public HashMap read() throws FileNotFoundException, IOException {    	
		Properties properties = new Properties();
		BufferedInputStream stream = new BufferedInputStream(new FileInputStream(this.fileName));
		properties.load(stream);
		stream.close();

    	HashMap params = new HashMap();
    	
    	// Actions
		String doImportProcess = properties.getProperty("import");
		boolean doImport = false;
		if (doImportProcess != null) {
			doImport = Boolean.parseBoolean(doImportProcess.trim());
		} 
		params.put("doImport", doImport);
		logger.debug("doImport: " + doImport);
		
		String doExportProcess = properties.getProperty("export");
		boolean doExport = false;
		if (doExportProcess != null) {
			doExport = Boolean.parseBoolean(doExportProcess.trim());
		} 
		params.put("doExport", doExport);
		logger.debug("doExport: " + doExport);		
		
		String doAvwms = properties.getProperty("avwms");
		if (doAvwms != null) {
			params.put("doAvwms", doAvwms.trim());
		}
		logger.debug("doAvwms: " + doAvwms);
		
		String doIli2chProcess = properties.getProperty("ili2ch");
		boolean doIli2ch = false;
		if (doIli2chProcess != null) {
			doIli2ch = Boolean.parseBoolean(doIli2chProcess.trim());
		}
		params.put("doIli2ch", doIli2ch);
		logger.debug("doIli2ch: " + doIli2ch);
		
		String doGeobauProcess = properties.getProperty("geobau");
		boolean doGeobau = false;
		if (doGeobauProcess != null) {
			doGeobau = Boolean.parseBoolean(doGeobauProcess.trim());
		}
		params.put("doGeobau", doGeobau);
		logger.debug("doGeobau: " + doGeobau);		
		
		String doIli2freeframeProcess = properties.getProperty("ili2freeframe");
		boolean doIli2freeframe = false;
		if (doIli2freeframeProcess != null) {
			doIli2freeframe = Boolean.parseBoolean(doIli2freeframeProcess.trim());
		}
		params.put("doIli2freeframe", doIli2freeframe);
		logger.debug("doIli2freeframe: " + doIli2freeframe);
		
		String doMopublicProcess = properties.getProperty("mopublic");
		boolean doMopublic = false;
		if (doMopublicProcess != null) {
			doMopublic = Boolean.parseBoolean(doMopublicProcess.trim());
		}
		params.put("doMopublic", doMopublic);
		logger.debug("doMopublic: " + doMopublic);
		
		String doCommit2SvnProcess = properties.getProperty("commit2svn");
		boolean doCommit2Svn = false;
		if (doCommit2SvnProcess != null) {
			doCommit2Svn = Boolean.parseBoolean(doCommit2SvnProcess.trim());
		}
		params.put("doCommit2Svn", doCommit2Svn);
		logger.debug("doCommit2Svn: " + doCommit2Svn);
		
		String doDeleteFilesProcess = properties.getProperty("deletefiles");
		boolean doDeleteFiles = false;
		if (doDeleteFilesProcess != null) {
			doDeleteFiles = Boolean.parseBoolean(doDeleteFilesProcess.trim());
		}
		params.put("doDeleteFiles", doDeleteFiles);
		logger.debug("doDeleteFiles: " + doDeleteFiles);	
		
		
		// Import parameters		
		String importModelName = properties.getProperty("importModelName");
		if (importModelName != null) {
			params.put("importModelName", importModelName.trim());
		}
		logger.debug("importModelName: " + importModelName);
		
		String srcDir = properties.getProperty("importSourceDir");
		if (srcDir != null) {
			params.put("importSourceDir", srcDir.trim());
		}
		logger.debug("Import Source Directory: " + srcDir);
		
		String dstDir = properties.getProperty("importDestinationDir");
		if (dstDir != null) {
			params.put("importDestinationDir", dstDir.trim());
		}
		logger.debug("Import Destination Directory: " + dstDir);
		
		String additionalAttr= properties.getProperty("additionalAttributes");
		boolean addAttr = false;
		if (additionalAttr != null) {
			addAttr = Boolean.parseBoolean(additionalAttr.trim());
		}
		params.put("addAttr", addAttr);
		logger.debug("Additional Attributes: " + addAttr);
		
		String enumerationText = properties.getProperty("enumerationText"); 
		boolean enumText = false;
		if (enumerationText != null) {
			enumText = Boolean.parseBoolean(enumerationText.trim());
		}
		params.put("enumText", enumText);
		logger.debug("Enumeration Text: " + enumText);	
		
		String renumber = properties.getProperty("renumberTid");
		boolean renumberTid = false;
		if (renumber != null) {
			renumberTid = Boolean.parseBoolean(renumber.trim()); 
		}
		params.put("renumberTid", renumberTid);
		logger.debug("Renumber TID: " + renumberTid);		
		
		String ili2chDir = properties.getProperty("ili2chDir");
		if (ili2chDir != null) {
			params.put("ili2chDir", ili2chDir.trim());
		}
		logger.debug("Ili2ch Directory: " + ili2chDir);
		
		String ili2chModelName = properties.getProperty("ili2chModelName");
		if (ili2chModelName != null) {
			params.put("ili2chModelName", ili2chModelName.trim());
		}
		logger.debug("Ili2ch Modelname: " + ili2chModelName);

		String geobauDir = properties.getProperty("geobauDir");
		if (geobauDir != null) {
			params.put("geobauDir", geobauDir.trim());
		}
		logger.debug("geobauDir Directory: " + geobauDir);
		
		String ili2freeframeDir = properties.getProperty("ili2freeframeDir");
		if (ili2freeframeDir != null) {
			params.put("ili2freeframeDir", ili2freeframeDir.trim());
		}
		logger.debug("ili2freeframeDir Directory: " + ili2freeframeDir);
		
		String ili2freeframeModelName = properties.getProperty("ili2freeframeModelName");
		if (ili2freeframeModelName != null) {
			params.put("ili2freeframeModelName", ili2freeframeModelName.trim());
		}
		logger.debug("ili2freeframe Modelname: " + ili2freeframeModelName);
		
		String mopublicBaseDir = properties.getProperty("mopublicBaseDir");
		if (mopublicBaseDir != null) {
			params.put("mopublicBaseDir", mopublicBaseDir.trim());
		}
		logger.debug("mopublicBaseDir Directory: " + mopublicBaseDir);
		
		
		// Export parameters
		String exportModelName = properties.getProperty("exportModelName");
		if (exportModelName != null) {
			params.put("exportModelName", exportModelName.trim());
		}
		logger.debug("exportModelName: " + exportModelName);
				
		String exportDir = properties.getProperty("exportDestinationDir");
		if (exportDir != null) {
			params.put("exportDestinationDir", exportDir.trim());
		}
		logger.debug("Export Destination Directory: " + exportDir);


		
		// SVN parameters
		// Database parameters
    	String svnurl = properties.getProperty("svnurl");
    	if (svnurl != null) {
        	params.put("svnurl", svnurl.trim());
    	}
		logger.debug("svnurl: " + svnurl);
		
    	String svnpath = properties.getProperty("svnpath");
    	if (svnpath != null) {
        	params.put("svnpath", svnpath.trim());
    	}
		logger.debug("svnpath: " + svnpath);
		
    	String svnuser = properties.getProperty("svnuser");
    	if (svnuser != null) {
        	params.put("svnuser", svnuser.trim());
    	}
		logger.debug("svnuser: " + svnuser);
		
    	String svnpwd = properties.getProperty("svnpwd");
    	if (svnpwd != null) {
        	params.put("svnpwd", svnpwd.trim());
    	}
		logger.debug("svnpwd: " + svnpwd);
		
		
		// Database parameters
    	String host = properties.getProperty("dbhost");
    	if (host != null) {
        	params.put("host", host.trim());
    	}
		logger.debug("host: " + host);
	
    	String port = properties.getProperty("dbport");
    	if (port != null) {
        	params.put("port", port.trim());
    	}
		logger.debug("port: " + port);		
		
    	String dbname = properties.getProperty("dbname");
    	if (dbname != null) {
        	params.put("dbname", dbname.trim());
    	}
		logger.debug("port: " + dbname);		
		
    	String schema = properties.getProperty("dbschema");
    	if (schema != null) {
        	params.put("schema", schema.trim());
    	}
		logger.debug("schema: " + schema);		
		
    	String user = properties.getProperty("dbuser");
    	if (user != null) {
        	params.put("user", user.trim());
    	}
    	logger.debug("user: " + user);		
		
    	String pwd = properties.getProperty("dbpwd");
    	if (pwd != null) {
        	params.put("pwd", pwd.trim());
    	}
    	logger.debug("pwd: " + pwd);		

    	// Maintenance
    	String doVacuum = properties.getProperty("vacuum");
    	if (doVacuum != null) {
    		params.put("vacuum", doVacuum.trim());
    	}
    	logger.debug("vaccum: " + doVacuum);
    	
    	String doReindex = properties.getProperty("reindex");
    	if (doReindex != null) {
    		params.put("reindex", doReindex.trim());
    	}
    	logger.debug("reindex: " + doReindex);
    	
    	
    	// Gemeindefusionen
		String fusionText = properties.getProperty("fusion"); 
		boolean fusion = false;
		if (fusionText != null) {
			fusion = Boolean.parseBoolean(fusionText.trim());
		}
		params.put("doFusion", fusion);
		logger.debug("Fusion: " + fusion);	

    	return params;

    	
    }
    

}

