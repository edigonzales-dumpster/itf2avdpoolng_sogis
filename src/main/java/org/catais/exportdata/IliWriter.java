package org.catais.exportdata;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.catais.utils.Iox2wkt;
import org.catais.utils.ModelUtility;
import org.catais.utils.SegmentsExtracter;
import org.catais.utils.ViewableWrapper;
import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureSource;
import org.geotools.data.postgis.PostgisNGDataStoreFactory;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureCollections;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.awt.PointShapeFactory.Point;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineSegment;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.operation.linemerge.LineMerger;

import ch.interlis.ili2c.Ili2c;
import ch.interlis.ili2c.Ili2cException;
import ch.interlis.ili2c.config.Configuration;
import ch.interlis.ili2c.metamodel.*;
import ch.interlis.ilirepository.IliManager;
import ch.interlis.iom.IomObject;
import ch.interlis.iom_j.itf.EnumCodeMapper;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxWriter;
import ch.interlis.iox_j.StartBasketEvent;
import ch.interlis.iox_j.jts.Jts2iox;


public class IliWriter {
	private static Logger logger = Logger.getLogger(IliWriter.class);
	
	HashMap params = null;
	
    private String host = null;
    private String port = null;
    private String dbname = null;
    private String schema = null;
    private String user = null;
    private String pwd = null;
    
	private String exportModelName = null;
	private String exportDestinationDir = null;
	
	private ch.interlis.ili2c.metamodel.TransferDescription iliTd = null;
    private HashMap transferViewables = null; 
    private EnumCodeMapper enumCodeMapper = null;

    private int maxTid=0;
    
	private DataStore datastore = null;

    
	public IliWriter(HashMap params) throws IllegalArgumentException, IOException {
		logger.setLevel(Level.DEBUG);
		
		this.params = params;
		readParams();
		
		Map dbparams= new HashMap();
		dbparams.put("dbtype", "postgis");        
		dbparams.put("host", this.host);        
		dbparams.put("port", this.port);  
		dbparams.put("database", this.dbname); 
		dbparams.put("schema", this.schema);
		dbparams.put("user", this.user);        
		dbparams.put("passwd", this.pwd); 
		dbparams.put(PostgisNGDataStoreFactory.VALIDATECONN, true );
		dbparams.put(PostgisNGDataStoreFactory.MAX_OPEN_PREPARED_STATEMENTS, 100 );
		dbparams.put(PostgisNGDataStoreFactory.LOOSEBBOX, true );
		dbparams.put(PostgisNGDataStoreFactory.PREPARED_STATEMENTS, true );		 

		datastore = new PostgisNGDataStoreFactory().createDataStore(dbparams);
	}
	
	
	public void run() throws Ili2cException, IoxException, IOException, Exception {
		iliTd = this.getTransferDescription(exportModelName);
		transferViewables = ModelUtility.getItfTransferViewables(iliTd); 
		
        File outputFile = new File(exportDestinationDir, "276400.itf");
        IoxWriter ioxWriter = new ch.interlis.iom_j.itf.ItfWriter(outputFile, iliTd);
        ioxWriter.write(new ch.interlis.iox_j.StartTransferEvent("Interlis", null, null));
        
        writeItfBuffers(iliTd, ioxWriter);
        
        ioxWriter.write(new ch.interlis.iox_j.EndTransferEvent());
        ioxWriter.flush();
        ioxWriter.close();
        ioxWriter = null;
        maxTid = 0;
		

	}

	
	private void writeItfBuffers(ch.interlis.ili2c.metamodel.TransferDescription iliTd, IoxWriter ioxWriter) throws IoxException, IOException, Exception {
		Iterator modeli = iliTd.iterator();
		int topicNr=0;
		while (modeli.hasNext()) {
			Object mObj = modeli.next();
			if (mObj instanceof Model) {
				Model model = (Model) mObj;
				if (model instanceof TypeModel) {
					continue;
				}
				if (model instanceof PredefinedModel) {
					continue;
				}
				Iterator topici = model.iterator();
				while (topici.hasNext()) {
					Object tObj = topici.next();
					if (tObj instanceof Topic) {
						Topic topic = (Topic) tObj;
						//System.out.println(topic.toString());
						// StartBasket
						topicNr++;
						StartBasketEvent basketEvent = new ch.interlis.iox_j.StartBasketEvent(topic.getScopedName(null),Integer.toString(topicNr));
						ioxWriter.write(basketEvent);
						Iterator iter = topic.getViewables().iterator();
						while (iter.hasNext()) {
							Object obj = iter.next();
							if (obj instanceof Viewable) {
								Viewable v = (Viewable) obj;
								if((v instanceof Table) && !((Table)v).isIdentifiable()){
									// STRUCTURE
									continue;
								}
								if(ModelUtility.isPureRefAssoc(v)){
									continue;
								}
								Iterator attri = null;
								String className = v.getScopedName(null);
								ViewableWrapper wrapper = (ViewableWrapper) transferViewables.get(className);

								// Name der Tabelle wie sie in Postgis heisst.
								String featureName = (className.substring(className.indexOf(".")+1)).replace(".", "_").toLowerCase();
								logger.debug(featureName);

								// Area Line Tables
								if(wrapper.getGeomAttr4FME() != null && wrapper.getGeomAttr4FME().getDomainResolvingAliases() instanceof AreaType){
									// build line table from polygons/donuts
									String lineTableName =
											v.getContainer().getScopedName(
													null)
													+ "."
													+ v.getName()
													+ "_"
													+ wrapper.getGeomAttr4FME().getName();

									writeItfLineTableArea(ioxWriter, featureName, lineTableName);
								}

								// Main Table
								writeBasket(ioxWriter, className, featureName);

								// Surface Line Tables
								if(wrapper.getGeomAttr4FME()!=null && wrapper.getGeomAttr4FME().getDomainResolvingAliases() instanceof SurfaceType){
									// build line table from polygons/donuts
									String lineTableName =
											v.getContainer().getScopedName(
													null)
													+ "."
													+ v.getName()
													+ "_"
													+ wrapper.getGeomAttr4FME().getName();

									writeItfLineTableSurface(ioxWriter, featureName, lineTableName);
								}
							}
						}
						ioxWriter.write(new ch.interlis.iox_j.EndBasketEvent());
					}       
				}
			}
		}
	}
	
	
	private void writeItfLineTableArea(IoxWriter ioxWriter, String mainTableName, String lineTableName) throws IoxException, Exception {
		FeatureSource source = datastore.getFeatureSource(mainTableName);
		FeatureCollection fc = source.getFeatures();
		logger.debug("Groesse: " + fc.size());

		if(fc != null) {
			SegmentsExtracter extracter = new SegmentsExtracter();
			Collection segmentsList = extracter.getSegments(fc);

			ArrayList lines = new ArrayList();

			GeometryFactory fact = new GeometryFactory();
			LineMerger lineMerger = new LineMerger(); 
			for (Iterator i = segmentsList.iterator(); i.hasNext();) {
				LineSegment seg = (LineSegment) i.next();
				Coordinate p0 =  new Coordinate(seg.p0);
				Coordinate p1 =  new Coordinate(seg.p1);
				Coordinate[] coord = {p0, p1};
				LineString line = fact.createLineString(coord);
				lineMerger.add(line);
//				lines.add(line);

			}
			Collection mergedLines = lineMerger.getMergedLineStrings();
			
			
//			File newFile = new File("/tmp/"+mainTableName+".shp");
//			final SimpleFeatureType TYPE = DataUtilities.createType("Location",
//	                "location:LineString:srid=21781," + // <- the geometry attribute: Point type
//	                        "name:String," + // <- a String attribute
//	                        "number:Integer" // a number attribute
//	        );
//			ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
//
//	        Map<String, Serializable> params = new HashMap<String, Serializable>();
//	        params.put("url", newFile.toURI().toURL());
//	        params.put("create spatial index", Boolean.TRUE);
//
//	        ShapefileDataStore newDataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
//	        newDataStore.createSchema(TYPE);
//	        
//	        String typeName = newDataStore.getTypeNames()[0];
//	        SimpleFeatureSource featureSource = newDataStore.getFeatureSource(typeName);
//			
//	        SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;
//	        SimpleFeatureCollection collection = FeatureCollections.newCollection("foo");
	        
			ViewableWrapper wrapper=(ViewableWrapper)transferViewables.get(lineTableName);

			Iterator it = mergedLines.iterator();
//			Iterator it = lines.iterator();
			while (it.hasNext()) {
				
//				SimpleFeatureBuilder featBuilder = new SimpleFeatureBuilder( TYPE );	
//				featBuilder.set("name", "foo");
//				featBuilder.set("number", 1);
				
				LineString line = (LineString) it.next();
				
//				featBuilder.set("location", line);
//				SimpleFeature feature = featBuilder.buildFeature(null);
//				collection.add(feature);
				
				// Noch kein Linienattribute-Support.
				IomObject iomObj=new ch.interlis.iom_j.Iom_jObject(lineTableName,newTid());     
				String geomAttr = ch.interlis.iom_j.itf.ModelUtilities.getHelperTableGeomAttrName(wrapper.getGeomAttr4FME());
				IomObject polyline = Jts2iox.JTS2polyline(line);
				iomObj.addattrobj(geomAttr,polyline);

				ioxWriter.write(new ch.interlis.iox_j.ObjectEvent(iomObj));
			}
			
//			featureStore.addFeatures(collection);
			
		}
	}

	
	
	private void writeItfLineTableSurface(IoxWriter ioxWriter, String mainTableName, String lineTableName) throws Exception, IoxException {
		FeatureSource source = datastore.getFeatureSource(mainTableName);
		FeatureCollection fc = source.getFeatures();
		logger.debug("Groesse: " + fc.size());

		if(fc != null) {

			ViewableWrapper wrapper=(ViewableWrapper)transferViewables.get(lineTableName);
			AttributeDef attr=wrapper.getGeomAttr4FME();
			String iomAttrName=ch.interlis.iom_j.itf.ModelUtilities.getHelperTableGeomAttrName(attr);
			String fkName=ch.interlis.iom_j.itf.ModelUtilities.getHelperTableMainTableRef(attr);
			Type type = attr.getDomainResolvingAliases();                   

			FeatureIterator i = fc.features();
			try {
				while( i.hasNext() ) {
					SimpleFeature f = (SimpleFeature) i.next();
					Geometry g = (Geometry) f.getDefaultGeometry();
					
					if (g != null)
					{

						int numGeoms = g.getNumGeometries();
						for(int j=0; j<numGeoms; j++) {
	
							Polygon p = (Polygon) g.getGeometryN(j);
	
							// Shell of the Polygon
							{
								IomObject iomObj=new ch.interlis.iom_j.Iom_jObject(lineTableName, newTid());     
	
								//add ref to main table
								IomObject structvalue=iomObj.addattrobj(fkName,"REF");
								structvalue.setobjectrefoid(f.getAttribute("tid").toString());
	
								LineString shell = (LineString) p.getExteriorRing();
	
								IomObject polyline=Jts2iox.JTS2polyline(shell);
								iomObj.addattrobj(iomAttrName,polyline);
	
								// add line attributes
								SurfaceOrAreaType surfaceType=(SurfaceOrAreaType)type;
								addItfLineAttributes(iomObj, f, wrapper, surfaceType);
	
								ioxWriter.write(new ch.interlis.iox_j.ObjectEvent(iomObj));
							}
	
							// Holes of the Polygon
							int numHoles = p.getNumInteriorRing();
							for(int k=0; k<numHoles; k++) {
								IomObject iomObj=new ch.interlis.iom_j.Iom_jObject(lineTableName,newTid());     
	
								//add ref to main table
								IomObject structvalue=iomObj.addattrobj(fkName,"REF");
								structvalue.setobjectrefoid(f.getAttribute("tid").toString());
	
								LineString hole = (LineString) p.getInteriorRingN(k);
	
								IomObject polyline=Jts2iox.JTS2polyline(hole);
								iomObj.addattrobj(iomAttrName,polyline);
	
								// add line attributes
								SurfaceOrAreaType surfaceType=(SurfaceOrAreaType)type;
								addItfLineAttributes(iomObj, f, wrapper, surfaceType);
	
								ioxWriter.write(new ch.interlis.iox_j.ObjectEvent(iomObj));
	
							}
						}
					}
				}
			}  finally {
				fc.close( i );
			}
		}
	}
	
	
	// Nur ein LINEATTR wird unterstützt.
	private IomObject addItfLineAttributes(IomObject iomObj, SimpleFeature feature, ViewableWrapper wrapper, SurfaceOrAreaType surfaceType) {
		Table lineAttrTable=surfaceType.getLineAttributeStructure();
		if(lineAttrTable!=null){
			IomObject obj = null;
			Iterator attri = lineAttrTable.getAttributes ();
			while(attri.hasNext()){
				AttributeDef lineattr=(AttributeDef)attri.next();
				obj = mapAttributeValue(feature, null, iomObj, wrapper, null, lineattr);
			}
			return obj;
		}
		return iomObj;
	}
	
	
	private IomObject mapAttributeValue(SimpleFeature feature, String attrPrefix, IomObject iomObj, ViewableWrapper wrapper, String geomattr, AttributeDef attr) {
		Type type = attr.getDomainResolvingAliases();
		String attrName=attr.getName();

		if(attrPrefix==null){
			attrPrefix="";
		}
		// ili2fme...
		//              if (type instanceof CompositionType){
		//                      System.out.println("compositiontype");
		//              } else if (type instanceof PolylineType){
		//                      System.out.println("polyline");
		//
		//              } else if(type instanceof SurfaceOrAreaType){
		//                      System.out.println("surfaceorarea");
		//              } else if(type instanceof CoordType){
		//                      System.out.println("coordtype");
		//              } else{
		//                      System.out.println("else");
		//              }

		Object value = feature.getAttribute(attrName.toLowerCase());
		if(value != null) {
			iomObj.setattrvalue(attrName, value.toString());
		} else {
			iomObj.setattrvalue(attrName, null);
		}
		return iomObj; 
	}
	
	
	private void writeBasket(IoxWriter ioxWriter, String className, String layerName) throws IOException {
		
		FeatureSource source = datastore.getFeatureSource(layerName);
		FeatureCollection fc = source.getFeatures();
		logger.debug("Groesse: " + fc.size());
		
		if(fc != null) {
			FeatureIterator i = fc.features();
			try {
				while(i.hasNext()) {                                  
					SimpleFeature f = (SimpleFeature) i.next();
					IomObject iomObj = mapFeature(className, f);
					try {
						ioxWriter.write(new ch.interlis.iox_j.ObjectEvent(iomObj));
					} catch(IoxException e) {
						logger.error(e.getMessage());
					}
				}
			} finally {
				fc.close(i);
			}
		}
	}
	
	
	private IomObject mapFeature(String className, SimpleFeature f) { 

		// Kanns das sein?
		String tid = (String) f.getAttribute("tid").toString();

		IomObject iomObj = new ch.interlis.iom_j.Iom_jObject(className,tid);    

		String tag=iomObj.getobjecttag();
		ViewableWrapper wrapper = ((ViewableWrapper) transferViewables.get(tag));

		Iterator iter;
		iter = wrapper.getAttrIterator();
		while (iter.hasNext()) {
			ViewableTransferElement obj = (ViewableTransferElement)iter.next();

			if (obj.obj instanceof AttributeDef) {
				AttributeDef attr = (AttributeDef) obj.obj;
				Type type = attr.getDomainResolvingAliases();                           
				String attrName = attr.getName();

				if(type instanceof AreaType) { 
//					Geometry g = (Geometry) f.getDefaultGeometry();
					Geometry g = (Geometry) f.getAttribute(attrName.toLowerCase());

					// Kein MultiPolygon-Support!
					Polygon p = null;
					p = (Polygon) g.getGeometryN(0);

					if(p != null) {          
						Coordinate mCoordinate = p.getInteriorPoint().getCoordinate();
						IomObject point = Jts2iox.JTS2coord(mCoordinate);
						iomObj.addattrobj(attrName, point);
					}

				} else if (type instanceof SurfaceType) {
					// do nothing...

				} else if (type instanceof PolylineType) {
					Geometry g = (Geometry) f.getAttribute(attrName.toLowerCase());
					// Kein richtiger MultiLineString-Support:
					// Falls Multilinestring vorliegt, wird
					// das erste Element gewählt.
					// String gtype = g.getGeometryType();
					// LineString line = null;
					// if(gtype.equalsIgnoreCase("MULTILINESTRING")) {
					//   line = (LineString) g.getGeometryN(0);
					// } else {
					//   line = (LineString) g;
					// }
					
					if (g != null) {
						LineString line = (LineString) g.getGeometryN(0);

						IomObject polyline = Jts2iox.JTS2polyline(line);                                                                                            
						iomObj.addattrobj(attrName,polyline);  
					}

				} else if (type instanceof CoordType) {                                 
					Geometry g = (Geometry) f.getDefaultGeometry();
					
					Coordinate mCoordinate = g.getInteriorPoint().getCoordinate();
					IomObject point = Jts2iox.JTS2coord(mCoordinate);
					iomObj.addattrobj(attrName,point);

				} else {
					Object value = f.getAttribute(attrName.toLowerCase());
					if(value != null) {
						if (value instanceof Integer) {
							DecimalFormat formater = new DecimalFormat("0");
							iomObj.setattrvalue(attrName, formater.format(value));
						} else {
							iomObj.setattrvalue(attrName, value.toString());
						}
					} else {
						iomObj.setattrvalue(attrName, null);
					}
				}
			}
			if (obj.obj instanceof RoleDef) {
				RoleDef role = (RoleDef) obj.obj;
				String roleName=role.getName();
				IomObject structvalue=iomObj.addattrobj(roleName,"REF");

				// Anscheinend gibts das Attribute nicht falls alle?? Werte NULL sind. 
				Object refObj = f.getAttribute(roleName.toLowerCase());
				if (refObj == null) {
					structvalue.setobjectrefoid("@");
				} else {
					String ref = refObj.toString();
					if (ref.isEmpty()) {
						structvalue.setobjectrefoid("@");
					} else {
						structvalue.setobjectrefoid(ref);
					}       
				}
			}
		}
		return iomObj;
	}
	
	
	private ch.interlis.ili2c.metamodel.TransferDescription getTransferDescription(String iliFile) throws Ili2cException {
    	IliManager manager = new IliManager();
    	String repositories[] = new String[]{"http://localhost/~stefan/models/", "http://www.sogeo.ch/models/", "http://models.geo.admin.ch/"};
    	manager.setRepositories(repositories);
    	ArrayList modelNames = new ArrayList();
    	modelNames.add(iliFile);
    	logger.debug(iliFile);
    	Configuration config = manager.getConfig(modelNames, 1.0);
    	ch.interlis.ili2c.metamodel.TransferDescription iliTd = Ili2c.runCompiler(config);

		if (iliTd == null) {
			throw new IllegalArgumentException("INTERLIS compiler failed");
		}
		return iliTd;   
	}

	
	private String newTid() {
		return Integer.toString(++maxTid);
	}
    
    
    private void readParams() throws IllegalArgumentException {
    	this.exportModelName = (String) params.get("exportModelName");
		logger.debug("exportModelName: " + this.exportModelName);
		if (this.exportModelName == null) {
			throw new IllegalArgumentException("exportModelName not set.");
		}	
		
		this.exportDestinationDir = (String) params.get("exportDestinationDir");
		logger.debug("Export Source Directory: " + exportDestinationDir);
		if (exportDestinationDir == null) {
			throw new IllegalArgumentException("Export source dir not set.");
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
    }
	
}
