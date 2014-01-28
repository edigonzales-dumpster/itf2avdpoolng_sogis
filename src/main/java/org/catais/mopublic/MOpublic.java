package org.catais.mopublic;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.catais.importdata.ImportData;
import org.catais.utils.IOUtils;
import org.geotools.data.DataStore;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureStore;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.postgis.PostgisNGDataStoreFactory;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.type.AttributeDescriptorImpl;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.VirtualTable;
import org.opengis.feature.Feature;
import org.opengis.feature.GeometryAttribute;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.AttributeType;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.PropertyDescriptor;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;

import ch.interlis.iox.IoxException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class MOpublic {
	private static Logger logger = Logger.getLogger(MOpublic.class);

	private HashMap params = null;
	
	private String host = null;
	private String port = null;
	private String dbname = null;
	private String schema = null;
	private String user = null;
	private String pwd = null;
	
	private String outputDir = null;

	private DataStore datastore = null;

	HashMap <String, FeatureCollection>collections = new HashMap(); 


	public MOpublic(HashMap params) throws Exception {
		logger.setLevel(Level.INFO);
		
		this.params = params;
		readParams();
			
		Map dbparams= new HashMap();
		dbparams.put("dbtype", "postgis");        
		dbparams.put("host", this.host);        
		dbparams.put("port", this.port);  
		dbparams.put("database", this.dbname); 
		dbparams.put("schema", "av_mopublic");
		dbparams.put("user", this.user);        
		dbparams.put("passwd", this.pwd); 
		dbparams.put(PostgisNGDataStoreFactory.VALIDATECONN, true );
		dbparams.put(PostgisNGDataStoreFactory.MAX_OPEN_PREPARED_STATEMENTS, 100 );
		dbparams.put(PostgisNGDataStoreFactory.LOOSEBBOX, true );
		dbparams.put(PostgisNGDataStoreFactory.PREPARED_STATEMENTS, true );		 

		datastore = new PostgisNGDataStoreFactory().createDataStore(dbparams);
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
		
    	this.schema = (String) params.get("schema");
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
		
		this.outputDir = (String) params.get("mopublicBaseDir");
		logger.debug("mopublicBaseDir: " + this.outputDir);
		if (this.outputDir == null) {
			throw new IllegalArgumentException("mopublicBaseDir not set.");
		}
	}

	public void write(String gemeinde, String language, String[] topics, String outputFormat, String frame) throws ClassNotFoundException, Exception {

		logger.debug("Lese Daten von: " + gemeinde);
		logger.debug("Outputformat: " + outputFormat);

		// Daten aus DB lesen.
		for (String topic : topics) {
			this.read(gemeinde, language, topic);
		}	

		// Daten in Outputformat schreiben.
		try {
			File tempDir = IOUtils.createTempDirectory("mopublic");

			if (outputFormat.equalsIgnoreCase("shp")) {
				this.saveAsShapefile(outputDir, language, gemeinde, frame, collections);
			} else if (outputFormat.equalsIgnoreCase("sqlite")) {
				this.saveAsSpatiaLite(outputDir, language, gemeinde, frame, collections);
			} 

//			String outFilename = outputDir + tempDir.separator + gemeinde + "_" + outputFormat + ".zip";
//			ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(outFilename));
//			IOUtils.zipDirectory(tempDir, zipOut, null);
//			zipOut.finish();

		} catch (ZipException e) {
			System.err.print("Could not create Zip-File");
		} catch (IOException e) {
			e.printStackTrace();
		} 
		datastore.dispose();
	}


	private void read(String gemeinde, String language, String topic) {

		ArrayList tables = this.getTableNamesFromTopicName(topic);
		logger.debug("Tables: " + tables);

		for (int i=0; i<tables.size(); i++) {
			logger.debug("Lesen Daten: " + topic + "__" + tables.get(i));
			this.read(gemeinde, language, topic, (String) tables.get(i));
		}
	}


	private void read(String gemeinde, String language, String topic, String table) {
		// Tabellen-Id
		double id = this.getClassIdFromClassName(topic, table);
//		logger.debug(id);

		// HashMap mit englischen Namen als Key und mit der gewuenschten Sprache als Value.
		HashMap <String, String>lookUp = this.getAttributeNames(id, language);
//		logger.debug(lookUp);

		// Fuer gewuenschte Tabelle die passende SQL-Query.
		String sql = this.getSqlQueryFromClassId(id);
//		logger.debug(sql);
				
		if (sql != null) {

			try {
	
				// Englische Attributnamen (Spaltennamen) ersetzen.
				// "replace" ist noch nicht ganz sauber, da
				// diese Methode "___pos_of" und "___pos" 
				// beide findet, wenn man "___pos" sucht.
				// Hier der Fall bei den HausnummerPos.
				// Ich versuchs mal mit angehängtem ",".
				// Nein doch nicht, da "pos_of" nicht uebersetzt werden muss.
				
				// Ah Mist, ___postalcode macht Probleme (___pos). Mach jetzt doch ein Komma dran.
				// ...
				for(String attrName : lookUp.keySet()) {			
					//System.out.println(attrName + ":   " + lookUp.get(attrName).toLowerCase());
					sql = sql.replace("___" + attrName.toLowerCase() + ",", lookUp.get(attrName) + ",");
				}
	
				// Alle "designation_e" aus dem String noch entfernen und mit 
				// passender Sprache ersetzen.
				sql = sql.replace("designation_e", "designation_" + language);
	
				// Falls im Modell keine BfS-Nr vorgesehen ist, muss diese in
				// der Query manuell ersetzt werden. 
				String fosnr = this.getTranslation("FOSNr", "lookup_tables_attribute_name", language);
				sql = sql.replace("___fosnr", fosnr);
				
	//			logger.debug("sql: " + sql);
	
				String vtName = topic+"__"+table;
				VirtualTable vt = new VirtualTable(vtName, sql);
	
				((JDBCDataStore) datastore).addVirtualTable(vt);
					
				FeatureSource source = datastore.getFeatureSource(vtName);
	
				// Nun ja: Das BfS-Nr.-Attribut heisst je nach Sprache anders. Bei den Hoehen
				// muss wohl via Geometrie gefiltert werden (Herrje, warum hat man hier keine
				// BfS-Nr????). -> try/except, zuerst über BfS-Nr. und anschliessend über Geometrie
				// filtern. Aber das lassen wir zuerst mal.
				// Die "anderen Hoheitsgrenzen" übrigens auch nicht....
				// Aha: mit Query sollte es gehen, da kann man typenames angeben:
				// http://docs.geotools.org/latest/tutorials/filter/query.html#id1
				// dh. falls im Modell KEINE BfSnr vorgesehen ist (Abfrage via lookUp
				// Tables), so muss Query verwenden werden.
	
	
				// Zum schneller und einfache filtern, hat bei uns jedes Objekte eine 
				// BfS-Nr. -> Wir müssen diese dort wieder entfernen wo sie gemäss
				// Modell nicht vorhanden ist wenn wir exportieren.
				// In LookUp Tables kann nachgeschaut werden, ob die BfS-Nr ein
				// Attribut der Tabelle ist. Falls nicht muss anstelle von "filter"
				// die "Query"-Klasse verwendet werden. Hier können Typename/Spalten-
				// Namen angegeben werden.
				// Was aber nicht geht ist die Kombination von keinem Geometrie-Attribute
				// und keinem BfS-Nr-Attribut im Modell. Da wir ja falls kein BfS-Nr-Attr.
				// vorhanden ist, die Attribute aus dem Modell nehmen (und da gibts dann auch
				// keine Geometrie-Attribut mehr...
				FeatureCollection fc = null;
				Query query = null;
	
				FilterFactory ff = CommonFactoryFinder.getFilterFactory( null );
				ff = CommonFactoryFinder.getFilterFactory( null );
	
				Filter filter = ff.equals(ff.property( fosnr.toLowerCase() ), ff.literal(gemeinde));	
	
				if (lookUp.containsKey("FOSNr")) {
					if (gemeinde == "kanton") {
						fc = source.getFeatures();
					} else {
						fc = source.getFeatures(filter);
					}
				} else {
	
					ArrayList propNames = new ArrayList();
	
					propNames.add("ogc_fid");
					propNames.add("tid");
	
					for(String attrName : lookUp.keySet()) {			
						propNames.add(lookUp.get(attrName).toLowerCase());
					}
	
					query = new Query();
					query.setTypeName(vtName);
					if (gemeinde != "kanton") {
						query.setFilter(filter);
					} 
					query.setPropertyNames(propNames);
	
					fc = source.getFeatures(query);
				}
	
				collections.put(vtName, fc);	
				logger.debug(vtName);
	
				logger.debug("Feature count: " + fc.size());
	
			} catch (IOException e) {
				e.printStackTrace();
				logger.error("IOException");
				logger.error(e.getMessage());
			} catch (NullPointerException e) {
				e.printStackTrace();
				logger.error("NullPointerException");
				logger.error(e.getMessage());
			}
		} else {
			logger.warn("No query found for: " + topic + "__" + table);
		}
	}
	
	
	private void saveAsShapefile(String outputDir, String language, String gemeinde, String frame, HashMap <String, FeatureCollection> collections) throws Exception, IOException {
		
		// Delete existing directory with all its content.
		String shpDirPath = outputDir + File.separator + "shp" + File.separator + frame.toLowerCase() + File.separator + language.toLowerCase() + File.separator + gemeinde;
		logger.info(shpDirPath);
		
		boolean success = false;
		if (new File(shpDirPath).isDirectory()) {
			success = IOUtils.delete(new File(shpDirPath));
			if (!success) {
				logger.error(shpDirPath + ": not deleted.");
				throw new Exception(shpDirPath + ": not deleted.");
			}
		}
		
		success = false;
		success = (new File(shpDirPath)).mkdirs();
		if (!success) {
			logger.error(shpDirPath + ": not created.");
			throw new Exception(shpDirPath + ": not created.");
		}
		
		ShapefileWriter shapefileWriter = new ShapefileWriter(shpDirPath, gemeinde, frame);
				
		for (String table : collections.keySet()) {
			logger.debug("Save As Shapefile: " + table);
			
			String topicName = table.split("__")[0];
			String tableName = table.split("__")[1];

			topicName = getTranslation(topicName, "lookup_tables_topic_name", language);
			tableName = getTranslation(tableName, "lookup_tables_classe_name", language);

			FeatureCollection fc = collections.get(table);	        
			shapefileWriter.write(table, topicName, tableName, fc);			
		}
		
		// We add also some metadata files to the zipfile.
		InputStream is =  MOpublic.class.getResourceAsStream("Hinweise.pdf");
		File pdfFile = new File(shpDirPath, "Hinweise.pdf");
		IOUtils.copy(is, pdfFile);

	
		String outFilename = shpDirPath + ".zip";
		ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(outFilename));
		IOUtils.zipDirectory(new File(shpDirPath), zipOut, null);
		zipOut.finish();
		
	}


	private void saveAsSpatiaLite(String outputDir, String language, String gemeinde, String frame, HashMap <String, FeatureCollection> collections) throws ClassNotFoundException, IOException, Exception {
		
		String sqliteDirPath = outputDir + File.separator + "sqlite" + File.separator + frame.toLowerCase() + File.separator + language.toLowerCase() + File.separator;
		logger.info(sqliteDirPath);
		
		SpatialLiteWriter spatialLite = new SpatialLiteWriter(sqliteDirPath, gemeinde, frame);

		for (String table : collections.keySet()) {
			logger.debug("Save As SpatialLite: " + table);

			String topicName = table.split("__")[0];
			String tableName = table.split("__")[1];

			topicName = getTranslation(topicName, "lookup_tables_topic_name", language);
			tableName = getTranslation(tableName, "lookup_tables_classe_name", language);

			FeatureCollection fc = collections.get(table);	        
			spatialLite.write(sqliteDirPath, gemeinde, table, topicName, tableName, fc);
		}
				
	}


	private ArrayList getTableNamesFromTopicName(String topic) {

		double id_topic = -1;
		ArrayList tables = new ArrayList();

		FeatureSource<SimpleFeatureType, SimpleFeature> source = null;
		FilterFactory ff = CommonFactoryFinder.getFilterFactory( null );		


		try {
			// Id des Topics
			source = datastore.getFeatureSource("lookup_tables_topic_name");
			Filter f1 = ff.equals(ff.property( "designation_e" ), ff.literal(topic));

			FeatureCollection fc1 = source.getFeatures(f1);

			FeatureIterator jt = fc1.features();
			try {
				while( jt.hasNext() ){
					Feature feature = jt.next();
					id_topic = ((Double) feature.getProperty("id_topic").getValue()).doubleValue();
				}
			}
			finally {
				jt.close();
			}

			// Tabellennamen
			source = datastore.getFeatureSource("lookup_tables_classe_name");

			ff = CommonFactoryFinder.getFilterFactory( null );
			Filter f2 = ff.equals(ff.property("id_topic"), ff.literal(id_topic));

			FeatureCollection fc2 = source.getFeatures(f2);

			FeatureIterator it = fc2.features();
			try {
				while( it.hasNext() ){
					Feature feature = it.next();
					tables.add((String) feature.getProperty("designation_e").getValue());
				}
			}
			finally {
				it.close();
			} 		 

		} catch (IOException e) {
			e.printStackTrace();
		}

		return tables;
	}


	private String getTranslation(String word, String table, String language) {

		try {
			FeatureSource<SimpleFeatureType, SimpleFeature> source = datastore.getFeatureSource(table);

			FilterFactory ff = CommonFactoryFinder.getFilterFactory( null );
			Filter filter = ff.equals(ff.property( "designation_e" ), ff.literal(word));

			FeatureCollection fc = source.getFeatures(filter);

			FeatureIterator it = fc.features();
			try {
				while( it.hasNext() ){
					Feature feature = it.next();
					return (String) feature.getProperty("designation_" + language).getValue();
				}
			}
			finally {
				it.close();
			} 		 
		} catch (IOException e) {
			e.printStackTrace();
		}

		return word;
	}


	private HashMap getAttributeNames(double table, String language) {

		HashMap lookUp = new HashMap();

		try {
			FeatureSource<SimpleFeatureType, SimpleFeature> source = datastore.getFeatureSource("lookup_tables_attribute_name");

			FilterFactory ff = CommonFactoryFinder.getFilterFactory( null );
			Filter filter = ff.equals(ff.property( "id_classe" ), ff.literal(table));

			FeatureCollection fc = source.getFeatures(filter);

			FeatureIterator it = fc.features();
			try {
				while( it.hasNext() ){
					Feature feature = it.next();
					lookUp.put((String) feature.getProperty("designation_e").getValue(), (String) feature.getProperty("designation_" + language).getValue());
				}
			}
			finally {
				it.close();
			} 		 
		} catch (IOException e) {
			e.printStackTrace();
		}

		return lookUp;

	}


	private double getClassIdFromClassName(String topic, String table) {

		FeatureSource<SimpleFeatureType, SimpleFeature> source = null;
		FilterFactory ff = CommonFactoryFinder.getFilterFactory( null );

		double topicId = -1;

		try {

			// Zuerst Id des Topics finden.
			source = datastore.getFeatureSource("lookup_tables_topic_name");
			Filter f1 = ff.equals(ff.property( "designation_e" ), ff.literal(topic));

			FeatureCollection fc1 = source.getFeatures(f1);

			FeatureIterator jt = fc1.features();
			try {
				while( jt.hasNext() ){
					Feature feature = jt.next();
					double id = ((Double) feature.getProperty("id_topic").getValue()).doubleValue();
					topicId = id;
				}
			}
			finally {
				jt.close();
			}

			// Nun die Id der Tabelle finden.
			source = datastore.getFeatureSource("lookup_tables_classe_name");
			Filter f2 = ff.equals(ff.property( "designation_e" ), ff.literal(table));
			Filter f3 = ff.equals(ff.property( "id_topic" ), ff.literal(topicId));
			ArrayList filters = new ArrayList();
			filters.add(f2);
			filters.add(f3);
			Filter filter = ff.and( filters );

			FeatureCollection fc2 = source.getFeatures(filter);

			FeatureIterator it = fc2.features();
			try {
				while( it.hasNext() ){
					Feature feature = it.next();
					double id = ((Double) feature.getProperty("id_classe").getValue()).doubleValue();
					return id;
				}
			}
			finally {
				it.close();
			} 		 
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}



	private String getSqlQueryFromClassId(Double table) {

		try {
			FeatureSource<SimpleFeatureType, SimpleFeature> source = datastore.getFeatureSource("lookup_tables_classe_queries");

			FilterFactory ff = CommonFactoryFinder.getFilterFactory( null );
			Filter filter = ff.equals(ff.property( "id_classe" ), ff.literal(table));

			FeatureCollection fc = source.getFeatures(filter);

			FeatureIterator it = fc.features();
			try {
				while( it.hasNext() ){
					Feature feature = it.next();
					String query = (String) feature.getProperty("sql_query").getValue();
					return query;
				}
			}
			finally {
				it.close();
			} 		 
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
}

