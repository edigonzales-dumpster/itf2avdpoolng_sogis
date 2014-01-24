package org.catais.ili2freeframe;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.catais.freeframe.FreeFrame;

import ch.interlis.ili2c.Ili2c;
import ch.interlis.ili2c.Ili2cException;
import ch.interlis.ili2c.config.Configuration;
import ch.interlis.ili2c.metamodel.*;
import ch.interlis.ilirepository.IliManager;
import ch.interlis.iom.*;
import ch.interlis.iox.*;
import ch.interlis.iom_j.itf.ItfReader;

public class Ili2FreeFrame {

	private static Logger logger = Logger.getLogger(Ili2FreeFrame.class);
	
	private ch.interlis.ili2c.metamodel.TransferDescription inputIliTd = null;
	private ch.interlis.ili2c.metamodel.TransferDescription outputIliTd = null;
	private Map tag2type = null;
	private String inputModelName = null;
	private String outputModelName = null;
	private IoxReader ioxReader = null;
	private IoxWriter ioxWriter = null;
	private FreeFrame freeFrame = null;

	public Ili2FreeFrame() {

	}


	public void transform(String inIli, String outIli, String inItf, String outItf) throws Ili2cException {
		logger.setLevel(Level.INFO);

		freeFrame = new FreeFrame();
		freeFrame.setDstRefFrame("LV95");
		freeFrame.buildSpatialIndex();

		inputIliTd = getTransferDescription(inIli);
		tag2type = ch.interlis.iom_j.itf.ModelUtilities.getTagMap(inputIliTd);
		inputModelName = inputIliTd.getLastModel().getName();

		outputIliTd = getTransferDescription(outIli);
		outputModelName = outputIliTd.getLastModel().getName();
		
		logger.info("InputModelName: " + inputModelName);
		logger.info("OutputModelName: " + outputModelName);

		
		try {
			ioxReader = new ch.interlis.iom_j.itf.ItfReader(new java.io.File(inItf));
			((ItfReader) ioxReader).setModel(inputIliTd);
			((ItfReader) ioxReader).setRenumberTids(false);
			((ItfReader) ioxReader).setReadEnumValAsItfCode(false);

			File itfOutputFile = new File(outItf);
			ioxWriter = new ch.interlis.iom_j.itf.ItfWriter(itfOutputFile, outputIliTd);
			ioxWriter.write(new ch.interlis.iox_j.StartTransferEvent("Interlis", "AGI SO Interlis LV03 <-> LV95 Converter v0.0.1", null));

			int topicNr = 0;

			IoxEvent event = ioxReader.read();
			while (event!=null) {

				if(event instanceof StartBasketEvent) {
					StartBasketEvent inputBasketEvent = (StartBasketEvent) event;

					String basketName = inputBasketEvent.getType();	
					int firstPoint = basketName.indexOf(".");
					String outputClassName = outputModelName + basketName.substring(firstPoint);

					topicNr++;
					StartBasketEvent outputStartBasketEvent = new ch.interlis.iox_j.StartBasketEvent(outputClassName, Integer.toString(topicNr));
					ioxWriter.write(outputStartBasketEvent); 

				} else if(event instanceof ObjectEvent) {
					IomObject iomObj = ((ObjectEvent)event).getIomObject();
					
					transformGeometry(iomObj);
					
					String tag = iomObj.getobjecttag();
					String className = tag.substring(tag.indexOf(".")+1);	
					String tagOutput = outputModelName + "." + className;
					iomObj.setobjecttag(tagOutput);		
									
					try {
						ioxWriter.write(new ch.interlis.iox_j.ObjectEvent(iomObj));
					} catch (IoxException ioxe) {
						ioxe.printStackTrace();
					}

				} else if(event instanceof EndBasketEvent) {
					ioxWriter.write(new ch.interlis.iox_j.EndBasketEvent());

				} else if(event instanceof EndTransferEvent) {
					ioxReader.close();    

					ioxWriter.write(new ch.interlis.iox_j.EndTransferEvent());
					ioxWriter.flush();
					ioxWriter.close();    		    				

					break;
				}
				event = ioxReader.read();
			}
		} catch (IoxException ie) {
			logger.error(ie.getMessage());
			ie.printStackTrace();
		}
	}

	
	private void transformGeometry(IomObject iomObj) {

		Object tableObj = tag2type.get(iomObj.getobjecttag());		
	
		if (tableObj instanceof AbstractClassDef) {

			AbstractClassDef tableDef = (AbstractClassDef) tag2type.get(iomObj.getobjecttag());
			ArrayList attrs = ch.interlis.iom_j.itf.ModelUtilities.getIli1AttrList(tableDef);

			Iterator attri = attrs.iterator(); 
			while (attri.hasNext()) { 
				ViewableTransferElement obj = (ViewableTransferElement)attri.next();

				if (obj.obj instanceof AttributeDef) {
					AttributeDef attr = (AttributeDef) obj.obj;
					Type type = attr.getDomainResolvingAliases();                          
					String attrName = attr.getName();

					if (type instanceof SurfaceType) {  
    					String fkName = ch.interlis.iom_j.itf.ModelUtilities.getHelperTableMainTableRef(attr);
    					IomObject structvalue = iomObj.getattrobj(fkName, 0);
        				IomObject value = iomObj.getattrobj(ch.interlis.iom_j.itf.ModelUtilities.getHelperTableGeomAttrName(attr), 0);

        			} else if (type instanceof PolylineType) {
        				IomObject value = iomObj.getattrobj(attrName, 0);

        				if (value != null) {
        					try {
        						IomObject polyObj = freeFrame.transformPolyline(value);        																
        						iomObj.changeattrobj(attrName, 0, polyObj);        						
        					} catch (Exception e) {
        						e.printStackTrace();
        					}
        				}	
        			} else if (type instanceof CoordType || type instanceof AreaType) {
        				IomObject value = iomObj.getattrobj(attrName, 0);
        				
        				if (value != null) {
        					try {
            					IomObject pointObj = freeFrame.transformPoint(value);
            					iomObj.changeattrobj(attrName, 0, pointObj); 
        					} catch (Exception e) {
        						e.printStackTrace();
        					}
        				}
        			}
				}
			}
		}
		
		else if (tableObj instanceof LocalAttribute) {			
			LocalAttribute localAttr = (LocalAttribute) tag2type.get(iomObj.getobjecttag());	
			Type type = localAttr.getDomainResolvingAliases(); 
			
			if (type instanceof SurfaceType || type instanceof AreaType) {
				IomObject value = iomObj.getattrobj(ch.interlis.iom_j.itf.ModelUtilities.getHelperTableGeomAttrName(localAttr), 0);

				if (value != null) {
					try {
						IomObject polyObj = freeFrame.transformPolyline(value);																
						iomObj.changeattrobj(ch.interlis.iom_j.itf.ModelUtilities.getHelperTableGeomAttrName(localAttr), 0, polyObj);						
					} catch (Exception e) {
						e.printStackTrace();
					}
				}	
			}
		}
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

}
