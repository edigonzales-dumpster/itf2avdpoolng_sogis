package org.catais.maintenance;

import java.io.File;
import java.io.FilenameFilter;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Vacuum {
	private static Logger logger = Logger.getLogger(Vacuum.class);
	
	private HashMap params = null;
	
    private String host = null;
    private String port = null;
    private String dbname = null;
    private String schema = null;
    private String user = null;
    private String pwd = null;
    
    Connection conn = null;
    
    public Vacuum(HashMap params) throws ClassNotFoundException, SQLException {
    	logger.setLevel(Level.INFO);
    	
    	this.params = params;
    	readParams();
    	
		Class.forName("org.postgresql.Driver");	
		this.conn = DriverManager.getConnection("jdbc:postgresql://"+this.host+"/"+this.dbname, this.user, this.pwd);    	
    }
    
    
    public void run() throws SQLException {
    	if (conn != null) {

    		Statement s = null;
    		Statement v = null;
    		s = conn.createStatement();
    		v = conn.createStatement();

    		s.executeUpdate("SET work_mem TO '1GB';");
    		s.executeUpdate("SET maintenance_work_mem TO '512MB';");

    		String[] schemas = this.schema.split(",");
    		for (int i = 0; i < schemas.length; i++) {

    			ResultSet rs = null;
    			rs = s.executeQuery("SELECT 'VACUUM ANALYZE " + schemas[i] + ".' || relname || ';' FROM pg_class JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace WHERE nspname = '" + schemas[i] + "' AND relkind IN ('r');");

    			logger.info("Vacuumizing tables from: " + schemas[i]);
    			while (rs.next()) {
    				String sql = rs.getString(1);
    				logger.debug(sql);                              
    				int m = 0;
    				m = v.executeUpdate(sql);
    			}
    			rs.close();
    			logger.info("Vacuumizing finished.");

    		}

    		s.executeUpdate("SET work_mem TO '1MB';");
    		s.executeUpdate("SET maintenance_work_mem TO '16MB';");                 

    		s.close();
    		v.close();
    		conn.close(); 
    	}
    }
    
    
    private void readParams() {		
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
		
    	this.schema = (String) params.get("vacuum");
		logger.debug("vacuum schemas: " + this.schema);		
		if (this.schema == null) {
			throw new IllegalArgumentException("vacuum schema(s) not set.");
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

