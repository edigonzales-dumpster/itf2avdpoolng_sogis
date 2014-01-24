package org.catais.mopublic;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureCollections;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.Feature;
import org.opengis.feature.GeometryAttribute;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.AttributeType;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.PropertyDescriptor;

import org.catais.freeframe.FreeFrame;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class ShapefileWriter {
	private static Logger logger = Logger.getLogger(ShapefileWriter.class);

	String dirPath = null;
	String gem = null;
	String frame = null;
	org.catais.freeframe.FreeFrame freeFrame = null;
	
	
	public ShapefileWriter(String shpDirPath, String gemeinde, String referenceFrame) {
		dirPath = shpDirPath;
		gem = gemeinde;
		frame = referenceFrame;
			
		if (frame.equalsIgnoreCase("lv95")) {
			freeFrame = new org.catais.freeframe.FreeFrame();
			freeFrame.setDstRefFrame(frame);
			freeFrame.buildSpatialIndex();
		}
	}

	
	public void write(String table, String topicName, String tableName, FeatureCollection fc) throws Exception {
		
		File file = new File(dirPath + File.separator + topicName + "__" + tableName + ".shp");

		// Weil die Daten aus einer VirtualTable stammen (und somit nicht in der geometry_columns sind, ist die
		// Geometrie vom Type "Geometry". So kann man aber keine Shapefiles schreiben. -> Erstes Feature lesen -> Geometrie-Typ
		// bestimmen -> FeatureType bis auf Geometrie kopieren und richtiger Geometrie-Typ setzen.

		try {
			GeometryAttribute geomAttr = null;
			FeatureIterator it = fc.features();
			try {
				while( it.hasNext() ){
					Feature feature = it.next();
					geomAttr = feature.getDefaultGeometryProperty();
					break;
				}
			}
			finally {
				it.close();
			} 	

			if (geomAttr != null) {
			
				SimpleFeatureType targetFeatureType = createTargetFeatureType((SimpleFeatureType) fc.getSchema(), geomAttr);
				
//				logger.debug("targetFeatureType: " + targetFeatureType);
				
				Map params = new HashMap();
				params.put("url", file.toURI().toURL());
				params.put("create spatial index", Boolean.FALSE);
	
				ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
				ShapefileDataStore newDataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
				newDataStore.createSchema(targetFeatureType);
				Charset charset = Charset.forName("UTF-8");
				newDataStore.setStringCharset(charset);
	
				Transaction transaction = new DefaultTransaction("create");
	
				String typeName = newDataStore.getTypeNames()[0];
				SimpleFeatureSource featureSource = newDataStore.getFeatureSource(typeName);
	
				if (featureSource instanceof SimpleFeatureStore) {
					SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;
	
					featureStore.setTransaction(transaction);
					try {
						if (frame.equalsIgnoreCase("lv95")) {
							FeatureCollection fcTrans = transform(fc);
							featureStore.addFeatures(fcTrans);
						} else {
							featureStore.addFeatures(fc);	
						}
						transaction.commit();
	
					} catch (Exception problem) {
						problem.printStackTrace();
						transaction.rollback();
	
					} finally {
						transaction.close();
					}
	
				} else {
					logger.error(typeName + " does not support read/write access");
				}
			} else {
				logger.debug("Empty featurecollection.");
			}
			
		} catch (MalformedURLException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		} catch (NullPointerException e) {
			logger.error(e.getMessage());
			logger.error("No Shapefile created: " + table);
		}
	}
	
	
	private FeatureCollection transform(FeatureCollection fc) {
		
		FeatureCollection collection = FeatureCollections.newCollection();

		FeatureIterator it = fc.features();
        try {
        	while(it.hasNext()) { 
        		
        		SimpleFeature f = (SimpleFeature) it.next();
        		        		
        		Geometry g = (Geometry) f.getDefaultGeometry();
        		if (g == null) {
        			collection.add(f);
        			continue;
        		}
        		
        		if(g instanceof MultiPolygon) {        			
        			int num = g.getNumGeometries();
        			Polygon[] polys = new Polygon[num];
        			for(int j=0; j<num; j++) {
        				polys[j] = freeFrame.transformPolygon((Polygon) g.getGeometryN(j));
        			}    
        			MultiPolygon multipoly = new GeometryFactory().createMultiPolygon(polys);
        			f.setDefaultGeometry(multipoly);
        			
        		} else if (g instanceof Polygon) {
        			Polygon poly = freeFrame.transformPolygon((Polygon) g);
        			f.setDefaultGeometry(poly);
        			
        		} else if (g instanceof MultiLineString) {
        			int num = g.getNumGeometries();
        			LineString[] lines = new LineString[num];
        			for(int j=0; j<num; j++) {
        				lines[j] = freeFrame.transformLineString((LineString) g.getGeometryN(j));
        			}    
        			MultiLineString multiline = new GeometryFactory().createMultiLineString(lines);
        			f.setDefaultGeometry(multiline);
        			
        		} else if (g instanceof LineString) {
        			LineString line = freeFrame.transformLineString((LineString) g);
        			f.setDefaultGeometry(line);

        		} else if (g instanceof MultiPoint) {
        			int num = g.getNumGeometries();
        			Point[] points  = new Point[num];
        			for(int j=0; j<num; j++) {
        				Coordinate coord = freeFrame.transformCoordinate(((Point) g.getGeometryN(j)).getCoordinate());
        				points[j] = new GeometryFactory().createPoint(coord);
        			} 
        			MultiPoint multipoint = new GeometryFactory().createMultiPoint(points);
        			f.setDefaultGeometry(multipoint);
        			
        		} else if (g instanceof Point) {
        			Coordinate coord = freeFrame.transformCoordinate(((Point) g).getCoordinate());
        			Point point = new GeometryFactory().createPoint(coord);
        			point.setSRID(2056);
        			f.setDefaultGeometry(point);
        		}
        		collection.add(f);
           	}
        } finally {
            it.close();
    	}
		return collection;
	}
	
	
	private SimpleFeatureType createTargetFeatureType(FeatureType sourceFeatureType, GeometryAttribute geomAttr) {

		SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName(sourceFeatureType.getName().getLocalPart());
		typeBuilder.setNamespaceURI(sourceFeatureType.getName().getNamespaceURI());
		
		if (frame.equalsIgnoreCase("lv95")) {
			typeBuilder.setSRS("EPSG:2056");
		} else {
			typeBuilder.setSRS("EPSG:21781");
		}
		
		for (PropertyDescriptor attbType : sourceFeatureType.getDescriptors()) {

			if (attbType instanceof GeometryDescriptor) {
				AttributeType at = ((GeometryDescriptor) attbType).getType();

				try {
					Geometry geom = (Geometry) geomAttr.getValue();

					if (geom instanceof Point)
						typeBuilder.add(at.getName().toString(), Point.class);

					else if (geom instanceof LineString)
						typeBuilder.add(at.getName().toString(), LineString.class);

					else if (geom instanceof Polygon)
						typeBuilder.add(at.getName().toString(), Polygon.class);

					else if (geom instanceof MultiPoint)
						typeBuilder.add(at.getName().toString(), MultiPoint.class);

					else if (geom instanceof MultiLineString)
						typeBuilder.add(at.getName().toString(), MultiLineString.class);

					else if (geom instanceof MultiPolygon)
						typeBuilder.add(at.getName().toString(), MultiPolygon.class);

					else 
						// Falls keine Geometrie-Attribut vorhanden ist, kann Geotools keine Shapes exportieren.
						// -> In der Query wird NULL::geometry requested, so hat FeatureType/Schema eine Geometrie. Da
						// es aber dazu keinen Wert gibt, muss als "else" eine Punktgeometrie dem Builder uebergeben werden.
						typeBuilder.add(at.getName().toString(), Point.class);
				} catch (NullPointerException e) {
					logger.error("createTargetFeatureType NullPointerException");
				}
			} else {
				typeBuilder.add((AttributeDescriptor)attbType);
			}
		}
		return typeBuilder.buildFeatureType();
	}
	
	
}
