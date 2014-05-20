package org.catais.postprocessing;

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
import org.catais.avwms.Avwms;

public class PostProcessing {
    private static Logger logger = Logger.getLogger(PostProcessing.class);

    private HashMap params = null;

    private String importSourceDir = null;
    private String sqlTable = "lookup_tables_sql";

    private String host = null;
    private String port = null;
    private String dbname = null;
    private String schema = "av_pfdgb";
    private String user = null;
    private String pwd = null;

    private ArrayList tables = new ArrayList();
    private ArrayList queries = new ArrayList();

    Connection conn = null;

    public PostProcessing(HashMap params) throws ClassNotFoundException, SQLException {
        logger.setLevel(Level.INFO);

        this.params = params;
        readParams();

        Class.forName("org.postgresql.Driver"); 
        this.conn = DriverManager.getConnection("jdbc:postgresql://"+this.host+"/"+this.dbname, this.user, this.pwd);
    }
    
    // ACHTUNG: Los-Nr. werden nicht berücksichtigt!!!!!!
    // Inhaltlich sollte das aber kein Problem sein, da über tid gejoined wird.
    // Wird halt - falls versch. Lose vorhanden sind - doppelt durchgeführt.

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
                logger.info("PostProcessing: " + dir.getAbsolutePath() + dir.separator + f);

                LinkedHashMap additionalAttributes = new LinkedHashMap();
                try {                
                    String gem_bfs = f.substring(0, 4);

                    Statement s = null;
                    s = conn.createStatement();

                    ResultSet rs = null;
                    // ACHTUNG: Order über ogc_fid!s
                    rs = s.executeQuery("SELECT * FROM " + this.schema + "." + this.sqlTable + " ORDER BY ogc_fid;");

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

                            // Steht jetzt in der Query selbst. Sonst gehen normale Updates nicht...
                            //String sqlDelete = "DELETE FROM " + this.schema + "." + table + " WHERE gem_bfs = " + gem_bfs + ";";
                            String sqlUpdate = (String) queries.get(i);                         

                            //t.executeUpdate(sqlDelete);
                            t.executeUpdate(sqlUpdate);
                            logger.debug(table);
                        }

                        conn.commit();
                        conn.setAutoCommit(true);
                        logger.info("Postprocessing: " + gem_bfs + " commited.");

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
