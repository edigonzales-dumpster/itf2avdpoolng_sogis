package org.catais.freeframe;

import org.catais.utils.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;
import org.geotools.geometry.jts.WKTReader2;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;
import com.vividsolutions.jts.geom.prep.PreparedPoint;
import com.vividsolutions.jts.geom.util.AffineTransformationBuilder;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.strtree.STRtree;

import ch.interlis.iom.*;
import ch.interlis.iox_j.jts.Iox2jts;
import ch.interlis.iox_j.jts.Jts2iox;


public class FreeFrame {
	
	private static Logger logger = Logger.getLogger(FreeFrame.class);
		
	private String srcRefFrame = "LV03";
	private String dstRefFrame = "LV95";
	private File srcChenyx06 = null;
	private InputStream is = null;
	private SimpleFeatureCollection srcFeatCollection = null;
	private SimpleFeatureSource srcFeatSource = null;
	private SpatialIndex spatialIndex = null;

	public FreeFrame() {
		createFeatureCollections();
	}
		
	
	public IomObject transformPoint(IomObject pointObj) throws Exception {
		
		logger.setLevel(Level.INFO);
		
		IomObject dstPointObj = null;
		
		Coordinate coord = Iox2jts.coord2JTS(pointObj);
//		logger.debug("coord: " + coord);	
		
		Coordinate dstCoord = transformCoordinate(coord);
//		logger.debug("dstCoord: " + dstCoord);
		
		Coordinate dstCoord3D = new Coordinate(dstCoord.x, dstCoord.y, coord.z);
//		logger.debug("dstCoord3D : " + dstCoord3D.toString());
		
		dstPointObj = Jts2iox.JTS2coord(dstCoord3D);
//		logger.debug("dstPointObj: " + dstPointObj);
		
		return dstPointObj;
	}
	
		
	public IomObject transformPolyline(IomObject polylineObj) throws Exception { 
		
		logger.setLevel(Level.OFF);
		
//		logger.debug("new polylineObj");
				
		if(polylineObj == null){
			logger.debug("polylineObj is null");
			return null;
		}
		
		IomObject dstPolylineObj = new ch.interlis.iom_j.Iom_jObject("POLYLINE", null);
		
		boolean clipped = polylineObj.getobjectconsistency()==IomConstants.IOM_INCOMPLETE;
		for(int sequencei = 0; sequencei < polylineObj.getattrvaluecount("sequence"); sequencei++) {
			if(!clipped && sequencei > 0) {
				throw new IllegalArgumentException();
			}
			
			IomObject sequence = polylineObj.getattrobj("sequence", sequencei);
//			logger.debug("sequence: " + sequence);
			
			IomObject dstSequence = new ch.interlis.iom_j.Iom_jObject("SEGMENTS", null);
			dstPolylineObj.addattrobj("sequence", dstSequence);
			
			for(int segmenti = 0; segmenti < sequence.getattrvaluecount("segment"); segmenti++) {
					
				IomObject segment = sequence.getattrobj("segment", segmenti);
//				logger.debug(segment.toString());
				
				if(segment.getobjecttag().equals("COORD")) {
					String c1 = segment.getattrvalue("C1");
					String c2 = segment.getattrvalue("C2");
					String c3 = segment.getattrvalue("C3");

					double xCoord;
					try{
						xCoord = Double.parseDouble(c1);
					} catch (Exception ex){
						throw new Exception("failed to read C1 <"+c1+">",ex);
					}
					
					double yCoord;
					try{
						yCoord = Double.parseDouble(c2);
					} catch (Exception ex){
						throw new Exception("failed to read C2 <"+c2+">",ex);
					}

					Coordinate coord = new Coordinate(xCoord, yCoord);
					Coordinate dstCoord = transformCoordinate(coord);
					
					IomObject dstCoordObj = new ch.interlis.iom_j.Iom_jObject("COORD", null);
					dstCoordObj.setattrvalue("C1", Double.toString(dstCoord.x));
					dstCoordObj.setattrvalue("C2", Double.toString(dstCoord.y));
					dstCoordObj.setattrvalue("C3", c3);

					dstSequence.addattrobj("segment", dstCoordObj);
				} else if (segment.getobjecttag().equals("ARC")) {
					String a1=segment.getattrvalue("A1");
					String a2=segment.getattrvalue("A2");
					String c1=segment.getattrvalue("C1");
					String c2=segment.getattrvalue("C2");
					String c3=segment.getattrvalue("C3");

					double xCoord;
					try{
						xCoord = Double.parseDouble(c1);
					} catch (Exception ex){
						throw new Exception("failed to read C1 <"+c1+">",ex);
					}

					double yCoord;
					try{
						yCoord = Double.parseDouble(c2);
					} catch (Exception ex){
						throw new Exception("failed to read C2 <"+c2+">",ex);
					}

					double arcXCoord;
					try{
						arcXCoord = Double.parseDouble(a1);
					} catch (Exception ex){
						throw new Exception("failed to read A1 <"+a1+">",ex);
					}

					double arcYCoord;
					try{
						arcYCoord = Double.parseDouble(a2);
					} catch (Exception ex){
						throw new Exception("failed to read A2 <"+a2+">",ex);
					}

					Coordinate coord = new Coordinate(xCoord, yCoord);
					Coordinate dstCoord = transformCoordinate(coord);

					Coordinate arcCoord = new Coordinate(arcXCoord, arcYCoord);
					Coordinate dstArcCoord = transformCoordinate(arcCoord);

					IomObject iomArc = new ch.interlis.iom_j.Iom_jObject("ARC", null);
					iomArc.setattrvalue("C1", Double.toString(dstCoord.x));
					iomArc.setattrvalue("C2", Double.toString(dstCoord.y));
					iomArc.setattrvalue("C3", c3);

					iomArc.setattrvalue("A1", Double.toString(dstArcCoord.x));
					iomArc.setattrvalue("A2", Double.toString(dstArcCoord.y));
//					logger.debug("iomArc: " + iomArc);
					
					dstSequence.addattrobj("segment", iomArc);
					
				} else {
					// custom line form is not supported
					logger.warn("custom line form is not supported");
				}
			
			}
			
//			logger.debug("dstPolylineObj: " + dstPolylineObj);

		}
		return dstPolylineObj;
	}

	
    public Polygon transformPolygon(Polygon p) {
    	
    	LineString shell = (LineString) p.getExteriorRing();
    	LineString shellTransformed = transformLineString(shell);
    	
    	LinearRing[] rings = new LinearRing[p.getNumInteriorRing()];
    	int num = p.getNumInteriorRing();
    	for(int i=0; i<num; i++) {
    		LineString line = transformLineString(p.getInteriorRingN(i));	
    		rings[i] = new LinearRing(line.getCoordinateSequence(), new GeometryFactory()); 
    	}    	    	
    	return new Polygon(new LinearRing(shellTransformed.getCoordinateSequence(), new GeometryFactory()), rings, new GeometryFactory());
    }
    
    
    public LineString transformLineString(LineString l) {
    	
    	Coordinate[] coords = l.getCoordinates();
    	int num = coords.length;

    	Coordinate[] coordsTransformed = new Coordinate[num];
    	for(int i=0; i<num; i++) {
    		coordsTransformed[i] = transformCoordinate(coords[i]);
    	}
    	CoordinateArraySequence sequence = new CoordinateArraySequence(coordsTransformed);
    	return new LineString(sequence, new GeometryFactory());
    }
    

	public Coordinate transformCoordinate(Coordinate coord) {
		logger.setLevel(Level.OFF);
		
    	Coordinate coordTransformed = new Coordinate();
    	Point point = new GeometryFactory().createPoint(coord);
    	PreparedPoint ppoint = new PreparedPoint(point.getInteriorPoint());

    	for (final Object o : spatialIndex.query(point.getEnvelopeInternal())) {
    		SimpleFeature f = (SimpleFeature) o;
    		MultiPolygon poly1 = (MultiPolygon) f.getDefaultGeometry();
    		if (ppoint.intersects(poly1)) {
    			String nummer = (String) f.getAttribute("nummer");
    			String dstWkt = (String) f.getAttribute("dstwkt");
    			
    			try {
        			Polygon poly2 = (Polygon) new WKTReader2().read(dstWkt);
        			
        			Coordinate[] t1 = poly1.getCoordinates();
        			Coordinate[] t2 = poly2.getCoordinates();
        			
    				AffineTransformationBuilder builder = new AffineTransformationBuilder(t1[0], t1[1], t1[2], t2[0], t2[1], t2[2]);
    				builder.getTransformation().transform(coord, coordTransformed);
    				DecimalFormat decimalForm = new DecimalFormat("#.###");
    				Coordinate coordTransRound = new Coordinate(Double.valueOf(decimalForm.format(coordTransformed.x)), Double.valueOf(decimalForm.format(coordTransformed.y)));
    				return coordTransRound;
        			
    			} catch (com.vividsolutions.jts.io.ParseException pe) {
    				pe.printStackTrace();
    				return coordTransformed;
    			} 			
    		}
    	}
    	return coordTransformed;
	}

	
	public void setDstRefFrame(String dst) {
		if (dst.equalsIgnoreCase("LV03")) {
			srcRefFrame = "LV95";
			dstRefFrame = "LV03";
		} else {
			srcRefFrame = "LV03";
			dstRefFrame = "LV95";
		}
		logger.debug("Destination reference frame: " + dstRefFrame);
	}
	
	
	public void buildSpatialIndex() {
		spatialIndex = new STRtree();
		
		logger.debug("Building SpatialIndex...");
		
        SimpleFeatureIterator iterator = srcFeatCollection.features();
        try {
            while( iterator.hasNext() ){
                SimpleFeature feature = iterator.next();
				MultiPolygon poly = (MultiPolygon) feature.getDefaultGeometry();
				spatialIndex.insert(poly.getEnvelopeInternal(), feature);
            }
        }
        finally {
            iterator.close();
        }

		logger.debug("SpatialIndex is built.");
	}	
	
	
	private void createFeatureCollections() {
		try {
			File tempDir = IOUtils.createTempDirectory("ili2freeframe");

			String chenyx06Prefix = null;
			if (srcRefFrame.equalsIgnoreCase("LV03")) {
				chenyx06Prefix = "chenyx06lv03";
			} else {
				chenyx06Prefix = "chenyx06lv95";
			}
				
			is =  FreeFrame.class.getResourceAsStream(chenyx06Prefix + ".dbf");   
			srcChenyx06 = new File(tempDir, chenyx06Prefix + ".dbf");
			IOUtils.copy(is, srcChenyx06);
			
			is =  FreeFrame.class.getResourceAsStream(chenyx06Prefix + ".shx");   
			srcChenyx06 = new File(tempDir, chenyx06Prefix + ".shx");
			IOUtils.copy(is, srcChenyx06);
			
			is =  FreeFrame.class.getResourceAsStream(chenyx06Prefix + ".shp");   
			srcChenyx06 = new File(tempDir, chenyx06Prefix + ".shp");
			IOUtils.copy(is, srcChenyx06);			

	        ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();

	        Map<String, Serializable> srcParams = new HashMap<String, Serializable>();
	        srcParams.put("url", srcChenyx06.toURI().toURL());
	        srcParams.put("create spatial index", Boolean.TRUE);

	        ShapefileDataStore srcDataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(srcParams);
	        srcFeatSource = srcDataStore.getFeatureSource();
	        srcFeatCollection = srcFeatSource.getFeatures();
	        	        
			logger.debug("Source CHENyx06 shapefile: " + srcChenyx06);
			logger.debug("Feature count of source CHENyx06 shapefile: " + srcFeatCollection.size());

			
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}		
	}
}
