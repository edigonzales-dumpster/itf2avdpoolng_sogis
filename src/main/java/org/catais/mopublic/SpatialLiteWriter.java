package org.catais.mopublic;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;
import java.util.zip.ZipOutputStream;

import org.apache.log4j.Logger;
import org.catais.utils.IOUtils;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.FeatureCollection;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.AttributeType;
import org.opengis.feature.type.GeometryDescriptor;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class SpatialLiteWriter {
	private static Logger logger = Logger.getLogger(SpatialLiteWriter.class);
	
	String dirPath = null;
	String gem = null;
	String frame = null;
	org.catais.freeframe.FreeFrame freeFrame = null;


	public SpatialLiteWriter(String sqliteDirPath, String gemeinde, String referenceFrame) throws ClassNotFoundException, IOException, Exception {
		dirPath = sqliteDirPath;
		gem = gemeinde;
		frame = referenceFrame;
			
		if (frame.equalsIgnoreCase("lv95")) {
			freeFrame = new org.catais.freeframe.FreeFrame();
			freeFrame.setDstRefFrame(frame);
			freeFrame.buildSpatialIndex();
		}
		
		this.copyTemplateDatabase(new File(sqliteDirPath), gemeinde);
		
		Class.forName("org.sqlite.JDBC");
	}
	
	
	public void write(String sqliteDirPath, String gemeinde, String table, String topicName, String tableName, FeatureCollection fc) throws SQLException {
				
		String typeName = topicName + "__" + tableName;
		
    	try {
        	        	
        	Connection connection = DriverManager.getConnection("jdbc:sqlite:" + sqliteDirPath + File.separatorChar + gemeinde + ".sqlite");
							
        	SimpleFeatureType sft = (SimpleFeatureType) fc.getSchema();
        	GeometryDescriptor gd = sft.getGeometryDescriptor();
        	List<AttributeDescriptor> atts = sft.getAttributeDescriptors();
		     
        	int epsg = 21781;
    		if (frame.equalsIgnoreCase("lv95")) {
    			epsg = 2056;
    		} else {
            	epsg = 21781;
    		}

		        // If the geometry of the table is not added in
		        // the goemetry columns we do not know what kind
		        // of geometry we need to add in the spatialite 
		        // db geometry columns.
		        // -> Find it out while writing the geometry into
		        // the table.
		        String geometryType = "POLYGON";
		   		        
//		        String requestName = curCollection.getSchema().getTypeName().toLowerCase();
//		        String typeName = requestName.replace(":", "__").replace(".", "-");
		            
		    // Drop and create Tabelle.
	        String create = this.createTableString(typeName, atts);
		        
	        Statement stmt = connection.createStatement();
	        try {
	        	stmt.executeUpdate(this.dropTableString(typeName));
	        } catch (Exception e) {}
	        stmt.executeUpdate(create);
	        stmt.close();
		        
		    // Features in die Tabelle schreiben.
	        String insert = insertIntoTableString(typeName, atts);
		        
	        boolean first = true;
	        
	        PreparedStatement pstmt = connection.prepareStatement(insert);
	        
	        SimpleFeatureIterator it = (SimpleFeatureIterator) fc.features();
	        try {
		        while(it.hasNext()) {
		        	SimpleFeature sf = (SimpleFeature) it.next();
		        	
		        	for (int i=0; i<sf.getAttributeCount(); i++) {
			        	Object attr = (Object) sf.getAttribute(i);
			       			        	
			        	AttributeType at = sf.getFeatureType().getType(i);
			        	
			            if (at.getBinding().equals(String.class)) {
			        		if (attr == null) {
			        			pstmt.setNull(i+1, 12);
			        		} else {
			        			pstmt.setString(i+1, (String) attr);
			        		}
			        		continue;
			            } else if (at.getBinding().equals(Integer.class)) {
			        		if (attr == null) {
			        			pstmt.setNull(i+1, 4);
			        		} else {
			        			pstmt.setInt(i+1, (Integer) attr);
			        		}
			        		continue;
			            } else if (at.getBinding().equals(Long.class)) {
			        		if (attr == null) {
			        			pstmt.setNull(i+1, -5);
			        		} else {
			        			pstmt.setLong(i+1, (Long) attr);
			        		}
		        			continue;
		        		} else if (at.getBinding().equals(Double.class)) {
			        		if (attr == null) {
			        			pstmt.setNull(i+1, 8);
			        		} else {
			        			pstmt.setDouble(i+1, (Double) attr);
			        		}
			        		continue;
			            } else if (at.getBinding().equals(java.util.Date.class) ||
			            		at.getBinding().equals(java.sql.Date.class)) {
			        		if (attr == null) {
			        			pstmt.setNull(i+1, 91);
			        		} else {
			        			//DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	                            DateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	                            java.util.Date timstamp = (java.sql.Date) attr;
	                            pstmt.setString(i+1, timestampFormat.format(timstamp));
			        		}
			        		continue;
			            } else if (at.getBinding().equals(java.math.BigDecimal.class)) {
			        		if (attr == null) {
			        			pstmt.setNull(i+1, 6);
			        		} else {
			        			pstmt.setFloat(i+1, ((BigDecimal) attr).floatValue());
			        		}
			        		continue;
			            }
			            else if (at.getBinding().equals(Point.class) || at.getBinding().equals(MultiPoint.class) ||
			            		at.getBinding().equals(LineString.class) || at.getBinding().equals(MultiLineString.class) || 
			            		at.getBinding().equals(Polygon.class) || at.getBinding().equals(MultiPolygon.class) ||
			            		at.getBinding().equals(Geometry.class)) {
			        		if (attr == null) {
			        			pstmt.setBytes(i+1, null);
			        		} else {
			        			if (first) {
			        				geometryType = getGeometryType( (Geometry) attr );
			        				first = false;
			        			}
			        			// assign a srid to the geometry
			        			Geometry geom = (Geometry) attr;
			        			
			        			// TODO: hier noch Freeframe.
			        			
//			        			geom.setSRID(epsg);
			        			// convert geometry into a special spatialite wkb
			        			byte[] wkb = new WKBWriter2(2,2).write((Geometry) attr);			        			
			        			pstmt.setBytes(i+1, wkb);
			        		}	
			        		continue;
			            }
			            else {
			            	// how to handle this properly???
			            	// how to forget the whole row???
			            	System.out.println("should not be here....");
			                pstmt.setNull(i+1, 	2004);
			            }
		        	}
		        	pstmt.addBatch();
		        }

	        } finally{
	            it.close();
	        }
	        
	        try {
	        	connection.setAutoCommit(false);
	        	pstmt.executeBatch();
	        	connection.setAutoCommit(true);
	        } catch (Exception e) {
	        	e.printStackTrace();
	        	connection.rollback();
	        }

	        pstmt.close();
		        
	        // Geometrie-Tabelle noch in die geometry_columsn schreiben.
	        if (gd != null) {
		        String geometryColumns = insertIntoGeometryColumnsString(typeName, gd, geometryType);
		        stmt = connection.createStatement();
		        stmt.executeUpdate(geometryColumns);
		        stmt.close();
	        }
		        
	        connection.close();
									
    	} catch(Exception e) {
    		e.printStackTrace();
    	}  finally {
//            try {
//                FileUtils.deleteDirectory(tempDir);
//            } catch(IOException e) {
//                LOGGER.warning("Could not delete temp directory: " + tempDir.getAbsolutePath() + " due to: " + e.getMessage());
//            }
        }
	}
	
	
    private String getGeometryType(Geometry geom) {
    	
    	String geometryType = "POLYGON";
		
    	if (geom instanceof Point)
			geometryType = "POINT";
		else if (geom instanceof LineString)
			geometryType = "LINESTRING";
		else if (geom instanceof Polygon)
			geometryType = "POLYGON";
		else if (geom instanceof MultiPoint)
			geometryType = "MULTIPOINT";
		else if (geom instanceof MultiLineString)
			geometryType = "MULTILINESTRING";
		else if (geom instanceof MultiPolygon)
			geometryType = "MULTIPOLYGON";
    	
    	return geometryType;
    }	
	
	
    private String insertIntoGeometryColumnsString(String type, GeometryDescriptor gd, String geometryType) {
    	
    	StringBuilder buf = new StringBuilder();
    	buf.append("INSERT INTO \"geometry_columns\" (f_table_name, f_geometry_column, type, coord_dimension, srid, spatial_index_enabled) VALUES");
    	buf.append(" (");
    	buf.append("\"").append(type).append("\", ");
    	buf.append("\"").append(gd.getLocalName()).append("\", ");
    	buf.append("\"").append(geometryType).append("\", ");
    	buf.append("2").append(", ");
    	int epsg = 0;
    	try {
    		epsg = CRS.lookupEpsgCode(gd.getCoordinateReferenceSystem(), false);
    	} catch (Exception e) {
//    		e.printStackTrace();
    		if (frame.equalsIgnoreCase("lv95")) {
    			epsg = 2056;
    		} else {
            	epsg = 21781;
    		}
    	}
    	buf.append(epsg).append(", ");
    	buf.append("0"); // kein Spatial-Index... Muss ich den auf 1 setzen, wenn ich R-Tree mache?
    	buf.append(")");
    	return buf.toString();
    }
    
    
private String insertIntoTableString(String type, List<AttributeDescriptor> atts) {
    	
    	StringBuilder buf = new StringBuilder();
    	buf.append("INSERT INTO \"").append(type).append("\"");
    	buf.append(" (");
    	
    	boolean first = true;
    	for (AttributeDescriptor ad : atts) {


    		AttributeType at = ad.getType();
    		if (first) {
    			first = false;
    		} else {
    			buf.append(", ");
    		}
    		buf.append("\"");
    		buf.append(ad.getLocalName().toLowerCase());
    		buf.append("\"");
    	}
    	
    	buf.append(") VALUES (");
    	
    	first = true;
    	for (AttributeDescriptor ad : atts) {
    		if (first == true) {
    			first = false;
    		} else {
    			buf.append(", ");
    		}
    		buf.append("?");
    	}
    	
    	buf.append(")");

    	return buf.toString();
    }    
	
	
    private String dropTableString(String type) {
    	StringBuilder buf = new StringBuilder();
    	buf.append("drop table \"").append(type).append('"');
    	return buf.toString();
    }    	
	
	
    private String createTableString(String type, List<AttributeDescriptor> atts) throws Exception {
    	
        StringBuilder buf = new StringBuilder();
        buf.append("CREATE TABLE \"").append(type);
        buf.append("\" (PK_UID INTEGER PRIMARY KEY AUTOINCREMENT, ");

        boolean first = true;
        for (AttributeDescriptor ad : atts) {
            AttributeType at = ad.getType();
            if (first) {
            	first = false;
            } else {
                buf.append(", ");
            }
            buf.append("\"");
            buf.append(ad.getLocalName().toLowerCase());
            buf.append("\" ");
            if (at.getBinding().equals(String.class)) {
                buf.append("VARCHAR");
            } else if (at.getBinding().equals(Integer.class)) {
                buf.append("INT");
            } else if (at.getBinding().equals(Long.class)) {
                buf.append("BIGINT");
            } else if (at.getBinding().equals(Double.class)) {
                buf.append("DOUBLE");
            } else if (at.getBinding().equals(java.util.Date.class)) {
                buf.append("DATE");
            } else if (at.getBinding().equals(java.sql.Date.class)) {
                buf.append("DATE");
            } else if (at.getBinding().equals(java.math.BigDecimal.class)) {
            	buf.append("DECIMAL");
            }
            else if (at.getBinding().equals(Point.class) || at.getBinding().equals(MultiPoint.class) ||
            		at.getBinding().equals(LineString.class) || at.getBinding().equals(MultiLineString.class) || 
            		at.getBinding().equals(Polygon.class) || at.getBinding().equals(MultiPolygon.class) ||
            		at.getBinding().equals(Geometry.class)) {
            	buf.append("GEOMETRY");
            }
            else {
                throw new Exception("Unknown type: " + at.getBinding());
            }
        }
        buf.append(')');
        return buf.toString();   
    } 	
	
	
    private void copyTemplateDatabase(File dir, String gemeinde) throws IOException, Exception {
    	
    	String db = "mopublic.sqlite";
    	String db_out = gemeinde + ".sqlite";
        InputStream dbstream = SpatialLiteWriter.class.getResourceAsStream(db);
        if (dbstream == null) {
            throw new RuntimeException("No library " + db); 
        }
        
        File dbfile = new File(dir, db_out);
        logger.debug(dbfile);
        try {
        	
        	IOUtils.copy(dbstream, dbfile);
            dbstream.close();
            logger.debug("Leere " + gemeinde + ".sqlite erzeugt.");

        } catch (Exception e) {
        	logger.error("Cannot copy empty database to: " + dbfile.toString());
        	logger.error(e.getMessage());
        	throw new Exception();
        }
    }	
}