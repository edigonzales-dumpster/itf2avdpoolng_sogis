package org.catais.importdata;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Collection;
import java.util.HashSet;
import java.math.BigDecimal;

import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureCollections;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
//import org.geotools.filter.text.cql2.*;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.postgis.PostgisNGDataStoreFactory;
//import org.geotools.data.postgis.PostgisDataStoreFactory;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureStore;
import org.geotools.data.Transaction;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.data.DataAccessFactory.Param;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.identity.FeatureId;
import org.opengis.filter.FilterFactory;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.PropertyIsEqualTo;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.expression.Literal;


import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.operation.polygonize.*;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.geom.prep.PreparedPoint;
import com.vividsolutions.jts.geom.util.LinearComponentExtracter;

import ch.interlis.ili2c.Ili2c;
import ch.interlis.ili2c.Ili2cException;
import ch.interlis.ili2c.config.Configuration;
import ch.interlis.ili2c.metamodel.*;
import ch.interlis.ilirepository.IliManager;
import ch.interlis.iom.*;
import ch.interlis.iox.*;
import ch.interlis.iom_j.itf.ItfReader;
import ch.interlis.iom_j.itf.EnumCodeMapper;
import ch.interlis.iox_j.jts.Iox2jts;
import ch.interlis.iox_j.jts.Iox2jtsException;
import ch.interlis.iox_j.jts.Jts2iox;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.catais.utils.Iox2wkt;
import org.catais.utils.ModelUtility;
import org.catais.utils.ViewableWrapper;



/**
 * Die Klasse IliReader stellt Methoden zum Lesen von Interlis1-Dateien zur Verfügung. 
 * 
 *
 * @author Stefan Ziegler
 * @version 0.1
 */

public class IliReader {
	private static Logger logger = Logger.getLogger(IliReader.class);
	
	private ch.interlis.ili2c.metamodel.TransferDescription iliTd = null;
	private int formatMode = 0;
	private final int MODE_XTF = 1;
	private final int MODE_ITF = 2;
	private HashMap transferViewables = null; 
	private HashMap tag2class = null; 
	private IoxReader ioxReader = null;
	private EnumCodeMapper enumCodeMapper = new EnumCodeMapper();

	private HashMap params = null;
	
	private String inputRepoModelName;
	private String itfFile;
		
	private String featureName = null;
	private LinkedHashMap collections = new LinkedHashMap();
    private String prefix = null;
    private LinkedHashMap featureTypes = null;
    private FeatureCollection<SimpleFeatureType, SimpleFeature> collection = null;
    private FeatureCollection<SimpleFeatureType, SimpleFeature> areaHelperCollection = null;
    private FeatureCollection<SimpleFeatureType, SimpleFeature> surfaceMainCollection = null;
    private FeatureStore<SimpleFeatureType, SimpleFeature> arcHelperStore = null;
    private GeometryFactory fact = new GeometryFactory();
    private LinkedHashMap additionalAttributes = new LinkedHashMap();

    private Boolean isAreaHelper = false;
    private Boolean isAreaMain = false;
    private String areaMainFeatureName = null;
    private Boolean isSurfaceHelper = false;
    private String surfaceMainFeatureName = null;
    private Boolean isSurfaceMain = false;
    private String geomName = null;
    private int tableNumber = 0;
    
    private String host = null;
    private String port = null;
    private String dbname = null;
    private String schema = null;
    private String user = null;
    private String pwd = null;
    
    private String itf = null;
    private String fosnr = null;
    private String lot = null;
    private String date = null;
    private Date dt = new Date();
    
    private boolean enumTxt = false;
    private boolean lv95geom = false;
    
    private Transaction t = null;
    
    private DataStore datastore = null;
    
	
    /**
     * @param iliFile Interlis1-Datenmodell
     */
    public IliReader(LinkedHashMap additionalAttributes, HashMap params) throws IllegalArgumentException, IOException, Exception {
    	logger.setLevel(Level.DEBUG);

    	this.additionalAttributes = additionalAttributes;
    	this.params = params;
    	
    	readParams();   
    	
    	this.collections.clear();
    }
    
    
    public void startTransaction() {
    	logger.debug("Starting Transaction...");
    	t = new DefaultTransaction();
    }
    
    
    public void commitTransaction() {
    	logger.debug("Committing Transaction...");
    	try {
        	try {
        		t.commit();
//            	logger.debug("Haha rollback...");
//        		t.rollback();
        	} catch (IOException ioe) {
        		logger.error("Cannot commit transaction");
        		logger.error(ioe.getMessage());
        		t.rollback();
        	} finally {
        		t.close();
        	}
    	} catch (IOException ioe) {
    		logger.error(ioe.getMessage());
    	}
    }
     
    
    private void readParams() {
    	
    	this.inputRepoModelName = (String) params.get("importModelName");
		logger.debug("inputRepoModelName: " + this.inputRepoModelName);
		if (this.inputRepoModelName == null) {
			throw new IllegalArgumentException("importModelName not set.");
		}	

		this.enumTxt = (Boolean) params.get("enumText");
		logger.debug("enumText: " + this.enumTxt);
		
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
		
		// fosnr, lot and date for single file import....
		// needs to be done.
    }
    
    
    /**
     * Kompiliert Interlis1-Datenmodell.
     * @throws Ili2cException 
     */
        
    public void compileModel() throws Ili2cException {    	
    	
    	IliManager manager = new IliManager();
    	String repositories[] = new String[]{"http://www.catais.org/models/", "http://www.sogeo.ch/models/", "http://models.geo.admin.ch/"};
    	manager.setRepositories(repositories);
    	ArrayList modelNames = new ArrayList();
    	modelNames.add(this.inputRepoModelName);
    	logger.info(this.inputRepoModelName);
    	Configuration config = manager.getConfig(modelNames, 1.0);
    	iliTd = Ili2c.runCompiler(config);
    	
    	
//    	ArrayList iliFiles = new ArrayList();
//    	ArrayList iliDirs = new ArrayList();
//    	iliFiles.add(iliFile.trim());
//
//    	// Compile ili models.
//    	iliTd = ch.interlis.ili2c.Main.compileIliFiles(iliFiles, iliDirs, null);
    	if (iliTd == null) {
    		throw new IllegalArgumentException("INTERLIS compiler failed");
    	}
    	logger.debug("Interlis model compiled");
    	
    	// INTERLIS 1?
    	if (iliTd.getIli1Format() != null) {
    		formatMode = MODE_ITF;
    	} else {
    		throw new IllegalStateException("unexpected formatMode");
    	}
    	
    	// TransferViewables
    	transferViewables = ModelUtility.getItfTransferViewables(iliTd);

    	// Tag2class
    	tag2class = ch.interlis.iom_j.itf.ModelUtilities.getTagMap(iliTd);
    }

    
    /**
     * @param prefix TID-Präfix
     */
    public void setTidPrefix(String prefix) {
    	this.prefix = prefix;
    }
    
    
    /**
     * Liest eine Interlis1-Transferdatei. 
     * <br><br>
     * Jede Tabelle wird in eine GeoTools FeatureCollection geschrieben. 
     * Diese ist über den Tabellennamen in einer LinkedHashMap referenziert.

     * @param itfFile Interlis1-Transferdatei
     * @param addAttr zusätzliche Attribute, die jedem Feature hinzugefügt wird (z.B. BfS-Nr.)
     * @param renumber falls true, werden die TIDs neu nummeriert
     *     
     * @return sämtliche Tabellen
     */

    public void read(String itfFile, boolean renumber, boolean lv95geom) {    	    	
    	featureTypes = ModelUtility.getFeatureTypesFromItfTransferViewables(iliTd, additionalAttributes, enumTxt, lv95geom);
    	
    	this.itfFile = itfFile;

    	try {
    		ioxReader = new ch.interlis.iom_j.itf.ItfReader(new java.io.File(this.itfFile));
    		((ItfReader) ioxReader).setModel(iliTd);
    		((ItfReader) ioxReader).setRenumberTids(renumber);
    		((ItfReader) ioxReader).setReadEnumValAsItfCode(true);

    		IoxEvent event = ioxReader.read();
    		while (event!=null) {
    			
    			if(event instanceof StartBasketEvent) {
    				StartBasketEvent basket = (StartBasketEvent) event;
    				logger.debug(basket.getType()+"(oid "+basket.getBid()+")...");

    			} else if(event instanceof ObjectEvent) {
    				IomObject iomObj = ((ObjectEvent)event).getIomObject();
    				String tag = iomObj.getobjecttag();
    				if (tag.equalsIgnoreCase(featureName)) {
    					readObject(iomObj, tag, false, true);	
    				} else {
    					readObject(iomObj, featureName, true, true);				
    				}
    				featureName = tag;	

    			} else if(event instanceof EndBasketEvent) {

    			}
    			else if(event instanceof EndTransferEvent) {
    				ioxReader.close();
    				
    				// Letzte Tabelle muss noch abgehandelt werden.    				
    				this.writeToPostgis();

    				// Hier ist noch ein Fehler im Import:
    				// "surfaces need a special treatment
                    // Das vorgehende 'writeToPostgis()' schreibt
                    // eigentlich die letzte Tabelle. FAlls die Surface-
                    // Tabelle keine Geometrien hat, wird sie solange nicht
                    // geschrieben bis wieder eine Surface-Tabelle kommt.
                    // Somit ist es möglich, dass sie gar nicht mehr 
                    // geschrieben wird (wenn keine weiter Surface-
                    // Tabelle kommt."
    				
    				datastore.dispose();
    				break;
    			}
    			try {
    				event = ioxReader.read();
    			} catch (IoxException e) {
    				logger.error("Fehler beim Lesen.");
    				logger.error(e.getMessage());
    				e.printStackTrace();
    			}

    		}                                               
    	} catch (Exception e) {
    		logger.error("Fehler beim Lesen.");
    		logger.error(e.getMessage());
    		e.printStackTrace();
    	}
    }
//	System.out.println(tag);

  
    /**
     * Wandelt ein IomObject in ein GeoTools-Feature um. 
     * Mehrere 'einfache' Geometrien (Punkte und Linien) pro
     * Tabelle werden unterstützt. 
     * 
     * @param iomObj IomObject
     * @param segmentize falls true, wird der Kreisbogen segmentiert
     */

    private void readObject(IomObject iomObj, String featureName, boolean changeCollection, boolean enumTxt) {
    	
    	String tag=iomObj.getobjecttag();
        	
    	// Bei einer Änderung der Tabelle können die
    	// Daten in die Datenbank geschrieben werden.
    	// Ausnahme: Areas und Surfaces. Da muss polygoniert
    	// und gejoined werden. Dh. bei Areas muss die Geometrie-Helfer
    	// Tabelle und bei Surfaces die Haupttabelle zwischengespeichert
    	// werden.

    	if (changeCollection == true) {
    		if (collection != null) {
    			
    			this.writeToPostgis();
    	
    		}
    		collection = FeatureCollections.newCollection(tag);
    	}
    	
        ViewableWrapper wrapper = null;
        wrapper = ((ViewableWrapper) transferViewables.get(tag));
                
        SimpleFeatureType ft = (SimpleFeatureType) featureTypes.get(tag);
        if (ft != null) {
        	
        	SimpleFeatureBuilder featBuilder = new SimpleFeatureBuilder( ft );	

        	// Falls die Objekte neu nummeriert
        	// werden sollen, wird noch ein Präfix
        	// hinzugefügt.
        	String tid = null;
        	if(((ItfReader) ioxReader).isRenumberTids()) {
        		tid = getTidOrRef(iomObj.getobjectoid());
        	} else {
        		tid = iomObj.getobjectoid();
        	}

        	featBuilder.set("tid", tid);

        	Iterator iter;
        	iter = wrapper.getAttrIterator();
        	while (iter.hasNext()) {
        		ViewableTransferElement obj = (ViewableTransferElement)iter.next();

        		if (obj.obj instanceof AttributeDef) {
        			AttributeDef attr = (AttributeDef) obj.obj;
        			Type type = attr.getDomainResolvingAliases();                          
        			String attrName = attr.getName();

        			// Fuer was ist das gut??
        			// Ich habe keine Ahnung!!
        			if (type instanceof CompositionType) {
        				logger.debug("CompositionType");
        				int valuec = iomObj.getattrvaluecount(attrName);
        				for (int valuei = 0; valuei < valuec; valuei++) {
        					IomObject value = iomObj.getattrobj(attrName, valuei);
        					if (value != null) {
        						// do nothing...                                                        
        					}
        				}
        			} else if (type instanceof PolylineType) {                               
        				IomObject value = iomObj.getattrobj(attrName, 0);
        				if (value != null) {
        					featBuilder.set(attrName.toLowerCase(), Iox2wkt.polyline2jts(value, 0));
        				} else {
        					featBuilder.set(attrName.toLowerCase(), null);
        				}
        			} else if (type instanceof SurfaceType) {        				
        				if(wrapper.isHelper()) {   
//        					System.out.println("********helper");
        					isSurfaceHelper = true;
        					String fkName=ch.interlis.iom_j.itf.ModelUtilities.getHelperTableMainTableRef(wrapper.getGeomAttr4FME());
        					IomObject structvalue=iomObj.getattrobj(fkName,0);
        					String refoid = null;
        					if(((ItfReader) ioxReader).isRenumberTids()) {
        						refoid = getTidOrRef(structvalue.getobjectrefoid());
        					} else {
        						refoid = structvalue.getobjectrefoid();
        					}
        					featBuilder.set("_itf_ref", refoid);
        				} else {
//        					System.out.println("********main");
        					isSurfaceMain = true;
        				}
						geomName = attrName.toLowerCase();
        				IomObject value = iomObj.getattrobj(ch.interlis.iom_j.itf.ModelUtilities.getHelperTableGeomAttrName(attr),0);
        				if (value != null) {
        					PrecisionDecimal maxOverlaps = ((SurfaceType) type).getMaxOverlap();
        					if (maxOverlaps == null) {
            					featBuilder.set(attrName.toLowerCase(), Iox2wkt.polyline2jts(value, 0.02));
        					} else {
            					featBuilder.set(attrName.toLowerCase(), Iox2wkt.polyline2jts(value, maxOverlaps.doubleValue()));
        					}
        				}
        			} else if (type instanceof AreaType) {                                                                  
        				if(wrapper.isHelper()) {
        					isAreaHelper = true;
        					String fkName=ch.interlis.iom_j.itf.ModelUtilities.getHelperTableMainTableRef(wrapper.getGeomAttr4FME());
        					IomObject structvalue=iomObj.getattrobj(fkName,0);
        					featBuilder.set("_itf_ref", null);                                  
        					IomObject value = iomObj.getattrobj(ch.interlis.iom_j.itf.ModelUtilities.getHelperTableGeomAttrName(attr),0);
        					if (value != null) {
        						PrecisionDecimal maxOverlaps = ((AreaType) type).getMaxOverlap();
            					if (maxOverlaps == null) {
                					featBuilder.set(attrName.toLowerCase(), Iox2wkt.polyline2jts(value, 0.02, arcHelperStore, tag, (Integer) this.additionalAttributes.get("gem_bfs"), (Integer) this.additionalAttributes.get("los")));
            					} else {
                					featBuilder.set(attrName.toLowerCase(), Iox2wkt.polyline2jts(value, maxOverlaps.doubleValue(), arcHelperStore, tag, (Integer) this.additionalAttributes.get("gem_bfs"), (Integer) this.additionalAttributes.get("los")));
            					}
        					}                                                               
        				} else {       					
        					isAreaMain = true;
        					IomObject value = iomObj.getattrobj(attrName, 0);
        					try {
        						Point point = new GeometryFactory().createPoint(Iox2jts.coord2JTS(value));
        						featBuilder.set(attrName.toLowerCase()+"_point", point);
        						geomName = attrName.toLowerCase();
        					} catch (Iox2jtsException e) {
        						e.printStackTrace();
        					}
        				}

        			} else if (type instanceof CoordType) {
        				IomObject value = iomObj.getattrobj(attrName, 0);
        				if(value!=null){
        					if(!value.getobjecttag().equals("COORD")){
        						logger.debug("Error: COORD");
        					} else {
        						try {
        							Point point = new GeometryFactory().createPoint(Iox2jts.coord2JTS(value));
        							featBuilder.set(attrName.toLowerCase(), point);
        						} catch (Iox2jtsException e) {
        							e.printStackTrace();
        						}                                                       
        					}
        				}
        			}else if (type instanceof NumericType) {
        				String value = iomObj.getattrvalue(attrName);
        				
//        				System.out.println(value.toString());
        				
        				if (value != null) {
        					featBuilder.set(attrName.toLowerCase(), Double.valueOf(value));
        				} else {
        					featBuilder.set(attrName.toLowerCase(), null);
        				}
        			} else if (type instanceof EnumerationType) {
        				String value = iomObj.getattrvalue(attrName);
        				if (value != null) {
        					featBuilder.set(attrName.toLowerCase(), Integer.valueOf(value));
        					if ( enumTxt == true ) {
        						featBuilder.set(attrName.toLowerCase()+"_txt", enumCodeMapper.mapItfCode2XtfCode((EnumerationType) type, value));
        					}
        				} else {
        					featBuilder.set(attrName.toLowerCase(), null);
        					if ( enumTxt == true ) {
        						featBuilder.set(attrName.toLowerCase()+"_txt", null);
        					}
        				}
        			} else {
        				String value = iomObj.getattrvalue(attrName);
        				if (value != null) {
        					featBuilder.set(attrName.toLowerCase(), value);
        				} else {
        					featBuilder.set(attrName.toLowerCase(), null);
        				}
        			} 
        		}
        		if (obj.obj instanceof RoleDef) {
        			RoleDef role = (RoleDef) obj.obj;
        			String roleName=role.getName();

        			IomObject structvalue = iomObj.getattrobj(roleName, 0);
        			String refoid = "";
        			if (structvalue != null) {
        				if(((ItfReader) ioxReader).isRenumberTids()) {
        					refoid = getTidOrRef(structvalue.getobjectrefoid());
        				} else {
        					refoid = structvalue.getobjectrefoid();
        				}
        			}
        			featBuilder.set(roleName.toLowerCase(), refoid.toString());
        		}
        	}

        	// Die zusätzlichen Attribute hinzufügen.
        	Iterator it = additionalAttributes.entrySet().iterator();
        	while(it.hasNext()) {
        		Map.Entry ientry = (Map.Entry)it.next();                
        		String attrName = ientry.getKey().toString();
        		featBuilder.set(attrName.toLowerCase(), ientry.getValue());
        	}        	
        	
        	SimpleFeature feature = featBuilder.buildFeature(null);	        	
        	
        	// Brauchts das?
        	if (collection == null) {
        		collection = FeatureCollections.newCollection(tag);
        	}
        	collection.add(feature);        	
        		
        } 
    }

    
    /**
     * TID wird mit Präfix ergänzt. Bei max. 12 Zeichen 
     * (falls FREE FORMAT im Datenmodell geht auch mehr)
     * sind somit 10 Mio. Objekt pro Transfer möglich. 
     *   
     * @param oriTid
     * @return TID mit Präfix
     */

    private String getTidOrRef(String oriTid) {
    	if(this.prefix != null) {
    		if(this.prefix.length() > 0) {
    			//System.out.println("prefix:"+this.prefix);
    			int tidc = oriTid.length();
    			for(int i=0;i<(8-tidc);i++) {
    				oriTid = "0" + oriTid ;
    			}       
    		}
    	}
    	if(this.prefix == null) prefix=""; 
    	return this.prefix+oriTid;
    }



	
    /**
     * Lorem Ipsum.
     * <br><br>
     * Die Helper-Tabelle wird gelöscht.
     * 
     * @param areaMainCollection Haupttabellenname
     * @param areaHelperCollection Helfertabellenname 
     * 
     * @return FeatureCollection / Main table mit Polygonen
     */
	
	
	private FeatureCollection areaBuilder(FeatureCollection areaMainCollection, FeatureCollection areaHelperCollection) {
		
		FeatureCollection<SimpleFeatureType, SimpleFeature> collection = null;
		
		if(areaMainCollection != null && areaHelperCollection != null) {
			
			collection = FeatureCollections.newCollection(areaMainCollection.getID());
			
			Polygonizer polygonizer = new Polygonizer();
			int inputEdgeCount = areaHelperCollection.size();
			Collection lines = getLines(areaHelperCollection);

			Collection nodedLines = lines;
			nodedLines = nodeLines((List) lines);

			for(Iterator it = nodedLines.iterator(); it.hasNext();) {
				Geometry g = (Geometry) it.next();
				polygonizer.add(g);
			}

			// Man könnte jetzt noch die nicht kleinen
			// Zipfel irgendeinem Polygon zuordnen.
			// Das erste, das das berührt?
			// Das kleinste?
			// Was geht am schnellsten?

			ArrayList polys = (ArrayList) polygonizer.getPolygons();

			final SpatialIndex spatialIndex = new STRtree();
			for (int i=0; i<polys.size();i++) {
				Polygon p = (Polygon) polys.get(i);
				spatialIndex.insert(p.getEnvelopeInternal(), p);
			}
			
			Iterator kt = areaMainCollection.iterator();
			while(kt.hasNext()) {
				SimpleFeature feat = (SimpleFeature) kt.next();
				Geometry point = (Geometry) feat.getAttribute(geomName+"_point");
				try {
					PreparedPoint ppoint = new PreparedPoint(point.getInteriorPoint());

					for (final Object o : spatialIndex.query(point.getEnvelopeInternal())) {
						Polygon p = (Polygon) o;
						if(ppoint.intersects(p)) {                                      
							feat.setAttribute(geomName, p);
							collection.add(feat);
						}
					} 
				} catch (NullPointerException e) {
					System.err.println("empty point");
				}                     
			}
			areaMainCollection.close(kt);
		}
		return collection;
	}
	


	
	/**
     * Es wird jedes Objekt einzeln polygoniert, da sich
     * Surfaces überlappen können. Dabei werden alle Objekte
     * mit identischer TID zwischengespeichert und anschliessend
     * der Polgonizer aufgerufen.
     * <br><br>
     * Die Helper-Tabelle wird gelöscht.
     * 
     * @param surfaceMainCollection Haupttabellenname
     * @param surfaceHelperCollection Helfertabellenname 
     * 
     * @return FeatureCollection / Main table mit Polygonen
     */

	private FeatureCollection surfaceBuilder(FeatureCollection surfaceMainCollection, FeatureCollection surfaceHelperCollection) {
		
		FeatureCollection<SimpleFeatureType, SimpleFeature> collection = null;

		HashMap features = new HashMap();

		if(surfaceMainCollection != null && surfaceHelperCollection != null) {

			collection = FeatureCollections.newCollection(surfaceMainCollection.getID());

			// Main-Table Features in HashMap mit
			// Key = TID.
			Iterator it = surfaceMainCollection.iterator();
			while(it.hasNext()) {
				SimpleFeature feat = (SimpleFeature) it.next();
				String tid = (String) feat.getAttribute("tid");
				features.put(tid, feat);
			}
			surfaceMainCollection.close(it);
			
			// Surface polygonieren
			LinkedHashMap lines = new LinkedHashMap();
			Iterator jt = surfaceHelperCollection.iterator();
			while(jt.hasNext()) {

				SimpleFeature feat = (SimpleFeature) jt.next();
				// An Stelle "1" steht bei Surface immer Beziehung zu
				// Main-Table.
				// 2010-01-30: Jetzt koennte man auch einfach
				// "_itf_ref" nehmen, da jetzt alles so heissen.
				String fk = (String) feat.getAttribute("_itf_ref");
				Geometry geom = (LineString) feat.getAttribute(geomName);

				if(lines.containsKey(fk)) {
					Geometry line1 = (Geometry) lines.get(fk);
					Geometry line2 = (Geometry) line1.union(geom);
					lines.put(fk, line2);                                          

				} else {
					lines.put(fk, geom);
				}
			}
			surfaceHelperCollection.close(jt);
			
			Iterator kt = lines.entrySet().iterator();
			while(kt.hasNext()) {
				Map.Entry kentry = (Map.Entry)kt.next();                
				String ref = kentry.getKey().toString();

				Polygonizer polygonizer = new Polygonizer();
				Geometry geom = (Geometry) kentry.getValue();

				polygonizer.add(geom);
				ArrayList polys = (ArrayList) polygonizer.getPolygons();

				// Darf eigentlich immer nur max. ein (1) Polygon
				// vorhanden sein!
				// Was aber anscheinend passieren kann, ist ein
				// LineString, der nicht polyoniert werden kann.
				// Falls keine Geometrie vorhanden ist, muss wieder
				// das original Feature geschreiben werden.

				// Wahrscheinlich macht polys.size() Probleme,
				// falls nicht grösser 0. Wird sich zeigen, bei
				// NFPerimetern!!!
				
				double exteriorPolyArea = 0;


				if(polys.size() > 0) {
					for(int i=0; i<polys.size();i++) {
						SimpleFeature feat = (SimpleFeature) features.get(ref);
						if(feat != null) {
							Polygon poly = (Polygon) polys.get(i);
							LinearRing[] interiorRings = new LinearRing[0];
							Polygon exteriorPoly = new Polygon((LinearRing)poly.getExteriorRing(),  interiorRings, new GeometryFactory());

							double exteriorPolyAreaTmp = exteriorPoly.getArea();
							if (exteriorPolyAreaTmp > exteriorPolyArea) {
								exteriorPolyArea = exteriorPolyAreaTmp;

								feat.setAttribute(geomName, (Polygon) polys.get(i));
								features.put(ref, feat);
							}
						}
					}   
				}
			}    
			
			Iterator lt = features.entrySet().iterator();
			while(lt.hasNext()) {
				Map.Entry lentry = (Map.Entry)lt.next();  
				SimpleFeature f = (SimpleFeature) lentry.getValue();
				collection.add(f);
			}
		}
		return collection;
	}

	

	
	/**
	 * Hilfsmethode Polygonieren / Knotenerzeugung
	 */
	private Collection nodeLines(Collection lines)
	{
		Geometry linesGeom = fact.createMultiLineString(fact.toLineStringArray(lines));
		Geometry unionInput  = fact.createMultiLineString(null);
		// force the unionInput to be non-empty if possible, to ensure union is not optimized away
		Geometry point = extractPoint(lines);
		if (point != null)
			unionInput = point;

		Geometry noded = linesGeom.union(unionInput);
		List nodedList = new ArrayList();
		nodedList.add(noded);
		return nodedList;
	}


	/**
	 * Hilfsmethode Polygonieren / Knotenerzeugung
	 */
	private Geometry extractPoint(Collection lines)
	{
		int minPts = Integer.MAX_VALUE;
		Geometry point = null;
		// extract first point from first non-empty geometry
		for (Iterator i = lines.iterator(); i.hasNext(); ) {
			Geometry g = (Geometry) i.next();
			if (! g.isEmpty()) {
				Coordinate p = g.getCoordinate();
				point = g.getFactory().createPoint(p);
			}
		}
		return point;
	}


	/**
	 * Hilfsmethode Polygonieren / Knotenerzeugung
	 */
	private Collection getLines(FeatureCollection inputFeatures)
	{
		List linesList = new ArrayList();
		LinearComponentExtracter lineFilter = new LinearComponentExtracter(linesList);
		for (Iterator i = inputFeatures.iterator(); i.hasNext(); ) {
			SimpleFeature f = (SimpleFeature) i.next();
			Geometry g = (Geometry) f.getDefaultGeometry();
			g.apply(lineFilter);
		}
		return linesList;
	}
	
	
    /**
     * Löscht sämtliche Features die gewisse Bedingungen
     * erfüllen. 

     * @param gem_bfs BfS-Nummer
     * @param los Teilgebiet
     * @param lieferdatum Datum der Lieferun
     */
	
	public void delete() throws IOException {
		
    	Map params= new HashMap();
		params.put("dbtype", "postgis");        
		params.put("host", this.host);        
		params.put("port", this.port);  
		params.put("database", this.dbname); 
		params.put("schema", this.schema);
		params.put("user", this.user);        
		params.put("passwd", this.pwd); 
		params.put(PostgisNGDataStoreFactory.VALIDATECONN, true );
		params.put(PostgisNGDataStoreFactory.MAX_OPEN_PREPARED_STATEMENTS, 100 );
		params.put(PostgisNGDataStoreFactory.LOOSEBBOX, true );
		params.put(PostgisNGDataStoreFactory.PREPARED_STATEMENTS, true );
		        	
		if (datastore == null) {
            datastore = new PostgisNGDataStoreFactory().createDataStore(params);
        }
		
		ArrayList tableNames = ModelUtility.getSQLTableNames(iliTd);
		
		for ( int i = 0; i < tableNames.size(); i++) {
			String tableName = (String) tableNames.get(i);

			try {
    			FeatureSource<SimpleFeatureType, SimpleFeature> source = datastore.getFeatureSource( tableName );
    			FeatureStore<SimpleFeatureType, SimpleFeature> store = (FeatureStore<SimpleFeatureType, SimpleFeature>) source;

    			store.setTransaction(t);

//    			try {
//    				System.out.println("Delete features: " + tableName + " WHERE gem_bfs = " + this.fosnr + " AND lot = " + this.lot + " AND lieferdatum = " + this.date);
    				logger.info("Delete features: " + tableName);

    				FilterFactory ff = CommonFactoryFinder.getFilterFactory( null );
    				Filter filter = null;
    				
    				if (this.additionalAttributes.size() > 0 ) {
    					// Nur noch nach gem_bfs und los löschen.    
        	    		Filter f1 = ff.equals(ff.property( "gem_bfs" ),  ff.literal(this.additionalAttributes.get("gem_bfs")));
        	    		Filter f2 = ff.equals(ff.property("los"), ff.literal(this.additionalAttributes.get("los")));    	    		
//        	    		Filter f3 = ff.equals(ff.property("lieferdatum"), ff.literal(this.date));
        	    		ArrayList filters = new ArrayList();
        	    		filters.add(f1);
        	    		filters.add(f2);
//        	    		filters.add(f3);
        	    		filter = ff.and( filters );
    				} else {
    					filter = Filter.INCLUDE;
    				}

    	    		store.removeFeatures(filter);
    				
    				
//    			} catch (Exception e) {
//    				e.printStackTrace();
//    				t.rollback();
//    			}
//    			finally {
//    				t.close();			
//    			}

			} catch (IOException ioe) {
//				System.err.println("Table \"" + tableName + "\" not found.");
				ioe.printStackTrace();
			}
		}
//		datastore.dispose();
	}

	
	
	
	
	/**
	 * Import alle FeatureCollections 
	 * in eine Postgis-DB.
	 */
	
	public void writeToPostgis(FeatureCollection collection, String featureName) {

		try {

			Map params= new HashMap();
			params.put("dbtype", "postgis");        
			params.put("host", this.host);        
			params.put("port", this.port);  
			params.put("database", this.dbname); 
			params.put("schema", this.schema);
			params.put("user", this.user);        
//			System.out.println(this.user + " " + this.pwd);
//			System.out.println(this.dbname + " " + this.schema);
			params.put("passwd", this.pwd); 
			params.put(PostgisNGDataStoreFactory.VALIDATECONN, true );
			params.put(PostgisNGDataStoreFactory.MAX_OPEN_PREPARED_STATEMENTS, 100 );
			params.put(PostgisNGDataStoreFactory.LOOSEBBOX, true );
			params.put(PostgisNGDataStoreFactory.PREPARED_STATEMENTS, true );

			if (datastore == null) {
                datastore = new PostgisNGDataStoreFactory().createDataStore(params);
            }
			
			String tableName = (featureName.substring(featureName.indexOf(".")+1)).replace(".", "_").toLowerCase();

			try {
				FeatureSource<SimpleFeatureType, SimpleFeature> source = datastore.getFeatureSource( tableName );
				FeatureStore<SimpleFeatureType, SimpleFeature> store = (FeatureStore<SimpleFeatureType, SimpleFeature>) source;

//				Transaction t = new DefaultTransaction();
				store.setTransaction(t);

//				try {    				
					logger.debug("Add features: "+featureName);
					store.addFeatures(collection);
//					t.commit();

//				} catch (Exception e) {
//					e.printStackTrace();
//					t.rollback();
//				}
//				finally {
//					t.close();			
//				}

			} catch (IOException ioe) {
				logger.debug("Table \"" + tableName + "\" not found.");
			}

//			datastore.dispose();

		} catch (IOException ioe) {
			ioe.printStackTrace();
		} 
	}
	
	
	private void writeToPostgis() {
		if ( isAreaHelper == true ) {
			
			areaHelperCollection = FeatureCollections.newCollection();
			areaHelperCollection.addAll(collection);
			
			isAreaHelper = false;
		}
		else if ( isAreaMain == true ) {
			if ( areaHelperCollection == null ) {
				this.writeToPostgis(collection, featureName);
			} else {
				if ( areaHelperCollection.size() == 0 ) {
					this.writeToPostgis(collection, featureName);
				} else {
    				FeatureCollection coll = areaBuilder(collection, areaHelperCollection);
    				    				
    				this.writeToPostgis(coll, featureName);
    				areaHelperCollection.clear();
    				coll.clear();
				}
			}

			isAreaHelper = false;
			isAreaMain = false;
		}
		else if ( isSurfaceMain == true ) {
			// Problem bei zwei aufeinanderfolgenden Surface-Tabellen.
			// Falls die erste KEINE Geometrie hat (keine Helper-Tabelle),
			// wird sie nicht in die DB geschrieben.
			// Wurde sie geschrieben, sollte die Anzahl = 0 sein.
			// Falls ungleich 0 -> in die DB schreiben.
			
			try {
				this.writeToPostgis(surfaceMainCollection, surfaceMainFeatureName);
				surfaceMainCollection.clear();
				
			} catch (NullPointerException e) {
				System.err.println("surfaceMainCollection keine features");
			}

			surfaceMainCollection = FeatureCollections.newCollection(collection.getID());
			surfaceMainCollection.addAll(collection);
			
			surfaceMainFeatureName = featureName;
			isSurfaceMain = false;
		}
		else if ( isSurfaceHelper == true ) {
			
			FeatureCollection coll = surfaceBuilder(surfaceMainCollection, collection);
			this.writeToPostgis(coll, surfaceMainFeatureName);
			
			surfaceMainCollection.clear();
			coll.clear();
			
			isSurfaceHelper = false;
			isSurfaceMain = false;
		}
		else {
//			System.out.println(featureName);
			this.writeToPostgis(collection, featureName);
			collection.clear();
			
			// BRAUCHE ICH DAS JETZT NOCH????? (siehe oben)
			
			// Falls keine Geometrie zur Surface Main Table
			// wird isSurfaceHelper nie true, somit
			// gibts die surfaceMainCollection immer noch und
			// sie muss noch in die DB geschrieben werden.
//			System.out.println("*1");
			if ( surfaceMainCollection != null ) { 
				if ( surfaceMainCollection.size() > 0 ) {
//					System.out.println("*2");				
					this.writeToPostgis(surfaceMainCollection, surfaceMainFeatureName);
					surfaceMainCollection.clear();
				}
			}
		}
		collection.clear();

	}
	

	
	public void getPostgisCreateTableStatements2(String schema, String adminuser, String selectuser, boolean lv95geom) {
		ModelUtility.getSQLStatements(iliTd, schema, adminuser, selectuser, this.additionalAttributes, this.enumTxt, "21781", lv95geom);
	}
	
	public void getTopicsXML(String schema) {
		ModelUtility.getTopicsXML(iliTd, schema);
	}

}