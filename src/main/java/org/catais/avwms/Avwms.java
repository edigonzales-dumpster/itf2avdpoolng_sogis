package org.catais.avwms;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Avwms {
	private static Logger logger = Logger.getLogger(Avwms.class);
	
	private HashMap params = null;
	
	private String importSourceDir = null;
	private boolean addAttr = false;
	private String sqlTable = "lookup_tables_sql";
	
    private String host = null;
    private String port = null;
    private String dbname = null;
    private String schema = null;
    private String user = null;
    private String pwd = null;

    private ArrayList tables = new ArrayList();
    private ArrayList queries = new ArrayList();

	Connection conn = null;
   
    
    public Avwms(HashMap params) throws ClassNotFoundException, SQLException {
    	logger.setLevel(Level.INFO);

    	this.params = params;
    	readParams();
    	
		Class.forName("org.postgresql.Driver");	
		this.conn = DriverManager.getConnection("jdbc:postgresql://"+this.host+"/"+this.dbname, this.user, this.pwd);
    }
    
    
    public void run() throws NullPointerException {
    	File dir = new File(importSourceDir);
    	String[] itfFileList = dir.list(new FilenameFilter() {
    		public boolean accept(File d, String name) {
    			return name.toLowerCase().endsWith(".itf"); 
    		}
    	});
    	logger.debug("Count of itf files: " + itfFileList.length);

    	if (itfFileList != null) {
    		for(String f : itfFileList) {
    			logger.info("AV-WMS: " + dir.getAbsolutePath() + dir.separator + f);

    			LinkedHashMap additionalAttributes = new LinkedHashMap();
    			try {
    			
//    				Integer.valueOf(f.substring(0, 4)).intValue();
					String gem_bfs = f.substring(0, 4);
					
					Statement s = null;
					s = conn.createStatement();
					
					ResultSet rs = null;
					this.schema = "av_avwmsde_t";
					rs = s.executeQuery("SELECT * FROM " + this.schema + "." + this.sqlTable + ";");
					
					queries.clear();
					tables.clear();
					
					while (rs.next()) {
						String sql = rs.getString("sql");
						String sql1 = sql.replace("___GEM_BFS", gem_bfs);
						logger.debug(sql1);
						queries.add(sql1);
						
						String table = rs.getString("table");
						tables.add(table);
					}	
					rs.close();
					s.close();

					Statement t = null;
					t = conn.createStatement();
					
					try {
						conn.setAutoCommit(false);
					
						for (int i=0; i<tables.size(); i++) {
	
							String table = (String) tables.get(i);
							// Da es in den AVWMS-Tables keine Los-Nummer gibt, kann nur jeweils die
							// ganze Gemeinde gelöscht werden. Darum muss dann auch die ganze Gemeinde
							// neu in die Tabelle eingefügt werden.
							// -> man könnte natürlich eine Losnummer einführen und das Attribut in QGIS verstecken...
							
							this.schema = "av_avwmsde_t";
							
							String sqlDelete = "DELETE FROM " + this.schema + "." + table + " WHERE bfsnr = " + gem_bfs + ";";
							String sqlInsert = (String) queries.get(i);							
							
							t.executeUpdate(sqlDelete);
							t.executeUpdate(sqlInsert);
							logger.debug(table);
						}
						
						conn.commit();
						conn.setAutoCommit(true);
						logger.info("AV-WMS: " + gem_bfs + " commited.");
						
					} catch(SQLException e) {
						logger.error(e.getMessage());
						conn.rollback();
					} 
					t.close();
    				
    			} catch (SQLException sqle) {
    				logger.error(sqle.getMessage());
    			} catch (IllegalArgumentException iae) {
    				logger.error(iae.getMessage());
    			} catch (IllegalStateException ise) {
    				logger.error(ise.getMessage());
    			} catch (Exception e) {
    				e.printStackTrace();
    				logger.error(e.getMessage());
    			}
    		}
    		try {
        		conn.close();
    		} catch (SQLException sqle) {
    			logger.error(sqle.getMessage());
    		}
    	}
    }
    
    
    private void readParams() {
		this.importSourceDir = (String) params.get("importSourceDir");
		logger.debug("Source Directory: " + this.importSourceDir);
		if (importSourceDir == null) {
			throw new IllegalArgumentException("Source dir not set.");
		}    	
		
		this.addAttr = (Boolean) params.get("addAttr");
		logger.debug("Additional Attributes: " + this.addAttr);	

    	this.host = (String) params.get("host");
		logger.debug("host: " + this.host);
		if (this.host == null) {
			throw new IllegalArgumentException("host not set.");
		}	
		
    	this.port = (String) params.get("port");
		logger.debug("port: " + this.port);		
		if (this.port == null) {
			throw new IllegalArgumentException("port not set.");
		}		
		
    	this.dbname = (String) params.get("dbname");
		logger.debug("port: " + this.dbname);		
		if (this.dbname == null) {
			throw new IllegalArgumentException("dbname not set.");
		}	
		
    	this.schema = (String) params.get("doAvwms");
		logger.debug("schema: " + this.schema);		
		if (this.schema == null) {
			throw new IllegalArgumentException("schema not set.");
		}			

    	this.user = (String) params.get("user");
		logger.debug("user: " + this.user);		
		if (this.user == null) {
			throw new IllegalArgumentException("user not set.");
		}	
    	
    	this.pwd = (String) params.get("pwd");
		logger.debug("pwd: " + this.pwd);		
		if (this.pwd == null) {
			throw new IllegalArgumentException("pwd not set.");
		}	
   }
}

