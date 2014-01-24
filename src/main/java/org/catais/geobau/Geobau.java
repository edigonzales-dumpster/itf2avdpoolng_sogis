package org.catais.geobau;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.catais.ili2ch.Ili2ch;
import org.geotools.data.DataStore;
import org.geotools.data.FeatureSource;
import org.geotools.data.postgis.PostgisNGDataStoreFactory;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.filter.Filter;
import org.geotools.filter.FilterFactory;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.VirtualTable;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.PropertyIsEqualTo;

import ch.interlis.ili2c.Ili2cException;

public class Geobau {
	private static Logger logger = Logger.getLogger(Geobau.class);
	
	private HashMap params = null;
	private String geobauDir = null; 
	private String importSourceDir = null;
	
	private String host = null;
	private String port = null;
	private String dbname = null;
	private String schema = null;
	private String user = null;
	private String pwd = null;

	
	public Geobau(HashMap params) {
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
    			logger.info("Geobau: " + f);    	
    			
    			try {
        			GeobauObj geobauObj = new GeobauObj(this.geobauDir, f);
    				
    				Map params= new HashMap();
    		        params.put("dbtype", "postgis");        
    		        params.put("host", this.host);        
    		        params.put("port", this.port);  
    		        params.put("database", this.dbname); 
    		        params.put("schema", "av_avdpool_ng");
    		        params.put("user", this.user);        
    		        params.put("passwd", this.pwd); 
    		        params.put(PostgisNGDataStoreFactory.VALIDATECONN, true );
    		        params.put(PostgisNGDataStoreFactory.MAX_OPEN_PREPARED_STATEMENTS, 100 );
    		        params.put(PostgisNGDataStoreFactory.LOOSEBBOX, true );
    		        params.put(PostgisNGDataStoreFactory.PREPARED_STATEMENTS, true );                

    		        DataStore datastore = new PostgisNGDataStoreFactory().createDataStore(params);
 				    				
        			ArrayList dxfQueries = this.getDxfQueries();
        			//logger.debug(dxfQueries);
        			
        			for (int i = 0; i < dxfQueries.size(); i++) {
        				String[] dxfQuery = (String[]) dxfQueries.get(i);
        				String vtName = dxfQuery[0];
        				String sql = dxfQuery[1];
        				logger.debug("layer_id: " + vtName);
        				logger.debug("sql: " + sql);
        				
                        VirtualTable vt = new VirtualTable(vtName, sql);
                        ((JDBCDataStore) datastore).addVirtualTable(vt);
                        FeatureSource source = datastore.getFeatureSource(vtName);
                        
                        org.opengis.filter.FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);
                		String fosnr = Integer.valueOf(f.substring(0, 4)).toString();
                        PropertyIsEqualTo filter = ff.equals(ff.property("gem_bfs"), ff.literal(fosnr)); 
                        logger.debug(filter.toString());
                        
                        FeatureCollection fc = source.getFeatures(filter);
                        
            			boolean header = false;
            			boolean footer = false;
            			
                        if (i == 0) header = true;
                        if (i == dxfQueries.size()-1) footer = true;
//                        logger.debug("header: " + header + " ... " + "footer: " + footer);
                        geobauObj.write(fc, vtName, 3, header, footer, sql);
        			}
        			

        			datastore.dispose();
                    geobauObj.zip();
        			logger.info("Geobau " + f + " finished.");
        			
    			} catch (NumberFormatException e) {
    				logger.error(e.getMessage());
    			} catch (FileNotFoundException e) {
    				logger.error(e.getMessage());
    			} catch (UnsupportedEncodingException e) {
    				logger.error(e.getMessage());
    			} catch (IOException e) {
    				logger.error(e.getMessage());
    			} catch (Exception e) {
    				logger.error(e.getMessage());
    			}
    		}
    	}
	}
	
	
	private ArrayList getDxfQueries() throws IOException, Exception {
		ArrayList dxfQueries =  new ArrayList();
		
		Map params= new HashMap();
        params.put("dbtype", "postgis");        
        params.put("host", this.host);        
        params.put("port", this.port);  
        params.put("database", this.dbname); 
        params.put("schema", "av_dxfgeobau_meta");
        params.put("user", this.user);        
        params.put("passwd", this.pwd); 
        params.put(PostgisNGDataStoreFactory.VALIDATECONN, true );
        params.put(PostgisNGDataStoreFactory.MAX_OPEN_PREPARED_STATEMENTS, 100 );
        params.put(PostgisNGDataStoreFactory.LOOSEBBOX, true );
        params.put(PostgisNGDataStoreFactory.PREPARED_STATEMENTS, true );                

        DataStore datastore = new PostgisNGDataStoreFactory().createDataStore(params);
        
        FeatureSource<SimpleFeatureType, SimpleFeature> source = datastore.getFeatureSource("lookup_tables_dxfgeobau_queries");        
        FeatureCollection fc = source.getFeatures();

        FeatureIterator jt = fc.features();
        try {
        	while( jt.hasNext() ){
        		Feature feature = jt.next();
        		
        		String[] dxfQuery = new String[2];
        		String layer_id = feature.getProperty("layer_id").getValue().toString();
        		String sql_query = feature.getProperty("sql_query").getValue().toString();
        		dxfQuery[0] = layer_id;
        		dxfQuery[1] = sql_query;
        		
        		dxfQueries.add(dxfQuery);
        	}
        }
        finally {
        	jt.close();
        }
        return dxfQueries;
	}
	
	
	private void readParams() {		
    	this.geobauDir = (String) params.get("geobauDir");
		logger.debug("geobauDir: " + this.geobauDir);		
		if (this.geobauDir == null) {
			throw new IllegalArgumentException("geobauDir not set.");
		}	
		
		this.importSourceDir = (String) params.get("importSourceDir");
		logger.debug("Import source Directory: " + this.importSourceDir);
		if (importSourceDir == null) {
			throw new IllegalArgumentException("Import source dir not set.");
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
		logger.debug("dbname: " + this.dbname);		
		if (this.dbname == null) {
			throw new IllegalArgumentException("dbname not set.");
		}	
		
		// This will be hardcoded....
//    	this.schema = (String) params.get("doAvwms");
//		logger.debug("schema: " + this.schema);		
//		if (this.schema == null) {
//			throw new IllegalArgumentException("schema not set.");
//		}			

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
