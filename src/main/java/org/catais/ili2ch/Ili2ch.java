package org.catais.ili2ch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.DecimalFormat;
import java.util.HashMap; 
import java.util.ArrayList; 
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import ch.interlis.ili2c.Ili2c;
import ch.interlis.ili2c.Ili2cException;
import ch.interlis.ili2c.config.Configuration;
import ch.interlis.ili2c.metamodel.*;
import ch.interlis.ilirepository.IliManager;
import ch.interlis.iom.*;
import ch.interlis.iox.*;
import ch.interlis.iom_j.itf.ItfReader;
import ch.interlis.iom_j.itf.ItfWriter;
import ch.interlis.iom_j.itf.EnumCodeMapper;
import ch.interlis.iom_j.itf.ModelUtilities;
import ch.interlis.iox_j.jts.Iox2jts;
import ch.interlis.iox_j.jts.Iox2jtsException;
import ch.interlis.iox_j.jts.Jts2iox;

public class Ili2ch {
	private static Logger logger = Logger.getLogger(Ili2ch.class);

	private ch.interlis.ili2c.metamodel.TransferDescription iliTdInput = null;
	private ch.interlis.ili2c.metamodel.TransferDescription iliTdOutput = null;
	private Map tag2type = null;
	private String inputModelName = null;
	private String outputModelName = null;
	private HashMap<String,EnumerationType> inputEnumerations = null;
	private HashMap<String,EnumerationType> outputEnumerations = null;
	private IoxReader ioxReader = null;
	private IoxWriter ioxWriter = null;
	private HashMap<String,HashMap> enumerationMappings = null;
	private ArrayList inputTables = null;
	private ArrayList outputTables = null;
	private EnumCodeMapper enumCodeMapper = new EnumCodeMapper();

	public Ili2ch() {

	}

	public void convert(String iliInput, String iliOutput, String itfInput, String itfOutput) throws Ili2cException {

		iliTdInput = this.getTransferDescription(iliInput);
		tag2type = ch.interlis.iom_j.itf.ModelUtilities.getTagMap(iliTdInput);
		inputModelName = iliTdInput.getLastModel().getName();
		inputEnumerations = this.getEnumerations(iliTdInput);


		iliTdOutput = this.getTransferDescription(iliOutput);
		outputModelName = iliTdOutput.getLastModel().getName();
		outputEnumerations = this.getEnumerations(iliTdOutput);

		enumerationMappings = this.mapEnumerationTypes(inputEnumerations, outputEnumerations);

		try {
			ioxReader = new ch.interlis.iom_j.itf.ItfReader(new java.io.File(itfInput));
			((ItfReader) ioxReader).setModel(iliTdInput);
			((ItfReader) ioxReader).setRenumberTids(false);
			((ItfReader) ioxReader).setReadEnumValAsItfCode(true);

			File itfOutputFile = new File(itfOutput);
			ioxWriter = new ch.interlis.iom_j.itf.ItfWriter(itfOutputFile, iliTdOutput);
			ioxWriter.write(new ch.interlis.iox_j.StartTransferEvent("Interlis", "AGI SO Interlis Converter v0.0.1", null));                        

			int topicNr = 0;

			IoxEvent event = ioxReader.read();
			while (event!=null) {

				if(event instanceof StartBasketEvent) {
					StartBasketEvent basket = (StartBasketEvent) event;

					String basketName = basket.getType();   
					int firstPoint = basketName.indexOf(".");
					String outputClassName = outputModelName + basketName.substring(firstPoint);

					topicNr++;
					StartBasketEvent outputStartBasketEvent = new ch.interlis.iox_j.StartBasketEvent(outputClassName, Integer.toString(topicNr));

					inputTables = ch.interlis.iom_j.itf.ModelUtilities.getItfTables(iliTdInput, basket.getType().split("\\.")[0], basket.getType().split("\\.")[1]);                                        
					outputTables = ch.interlis.iom_j.itf.ModelUtilities.getItfTables(iliTdOutput, outputStartBasketEvent.getType().split("\\.")[0], outputStartBasketEvent.getType().split("\\.")[1]);

					ioxWriter.write(outputStartBasketEvent); 

				} else if(event instanceof ObjectEvent) {
					IomObject iomObj = ((ObjectEvent)event).getIomObject();
					String tag = iomObj.getobjecttag();
					String tableName = tag.substring(tag.indexOf(".")+1);                                   

					// Was passiert falls schon Topic nicht existiert?
					if (this.isOutputTable(tableName) == true) {

						// Aufzaehlypen muessen u.U. ins
						// Zielmodell gemappt werden. 
						this.changeEnumerations(iomObj);

						String tagOutput = outputModelName + "." + tableName;
						iomObj.setobjecttag(tagOutput);                                         

						try {
							ioxWriter.write(new ch.interlis.iox_j.ObjectEvent(iomObj));
						} catch (IoxException ioxe) {
							ioxe.printStackTrace();
						}
					} else {
						// do nothing...
						// Tabelle existiert nicht im Bundesmodell.
					}

				} else if(event instanceof EndBasketEvent) {
					ioxWriter.write(new ch.interlis.iox_j.EndBasketEvent());
				}

				else if(event instanceof EndTransferEvent) {
					ioxReader.close();    

					ioxWriter.write(new ch.interlis.iox_j.EndTransferEvent());
					ioxWriter.flush();
					ioxWriter.close();                                              

					break;
				}
				event = ioxReader.read();
			}  
		} catch (IoxException ioxe) {
			logger.error(ioxe.getMessage());
			ioxe.printStackTrace();
		}
	}


	private boolean isOutputTable(String tableName) {

		for (int i = 0; i < outputTables.size(); i++) {
			Object o = (Object) outputTables.get(i);
			if (o instanceof Table) {
				String scopedOutputTableName = ((Table) o).getScopedName(null);
				String outputTableName = scopedOutputTableName.substring(scopedOutputTableName.indexOf(".")+1);
				if (tableName.equals(outputTableName)) {
					return true;
				}
			} else if (o instanceof LocalAttribute) {
				String scopedOutputTableName = ((LocalAttribute) o).getContainer().getScopedName(null);
				String attrName = ((LocalAttribute) o).getName();
				String outputTableName = scopedOutputTableName.substring(scopedOutputTableName.indexOf(".")+1) + "_" + attrName;
				if (tableName.equals(outputTableName)) {
					return true;
				}                               
			}
		}
		return false;
	}


	private void changeEnumerations(IomObject iomObj) {
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

					if (type instanceof EnumerationType) {
						String tag = iomObj.getobjecttag();
						String keyName = tag.substring(tag.indexOf(".")+1) + "." + attrName;

						HashMap enumerationMap = enumerationMappings.get(keyName);
						String attrValue = iomObj.getattrvalue(attrName);
						if (attrValue != null) {                                                        
							String Ctcode = String.valueOf(enumerationMap.get(Integer.parseInt(attrValue)));
							iomObj.setattrvalue(attrName, Ctcode);
						}
					}
				}
			}
		}
	}



	private ch.interlis.ili2c.metamodel.TransferDescription getTransferDescription(String iliFile) throws Ili2cException {
    	IliManager manager = new IliManager();
    	String repositories[] = new String[]{"http://www.catais.org/models/", "http://models.geo.admin.ch/"};
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



	private HashMap getEnumerations(ch.interlis.ili2c.metamodel.TransferDescription iliTd) {
		HashMap<String,EnumerationType> enumMap = new HashMap();

		Iterator modeli = iliTd.iterator();
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

					// Unnoetig, da jede Enumeration einmal fuer ein
					// Attribut verwendet wird. Und falls nicht, ist
					// sie nicht von Interesse.
					if (tObj instanceof Domain) {
						Type domainType = ((Domain) tObj).getType();

						if (domainType instanceof EnumerationType) {
							Enumeration enumeration = ((EnumerationType) domainType).getEnumeration();
						}                                                        
					}
					else if (tObj instanceof Topic) {
						Topic topic = (Topic) tObj;
						Iterator iter = topic.iterator();

						while (iter.hasNext()) {
							Object obj = iter.next();

							// Unnoetig, da jede Enumeration einmal fuer ein
							// Attribut verwendet wird. Und falls nicht, ist
							// sie nicht von Interesse.
							if (obj instanceof Domain) {
								Domain domain = (Domain) obj;
								Type domainType = domain.getType();     

								if (domainType instanceof EnumerationType) {
									Enumeration enumeration = ((EnumerationType) domainType).getEnumeration();
								}

								// Eigentlich nur das hier noetig.
								// Siehe oben.
							} else if (obj instanceof Viewable) {
								Viewable v = (Viewable) obj;

								if(ch.interlis.iom_j.itf.ModelUtilities.isPureRefAssoc(v)){
									continue;
								}

								java.util.List attrv = ch.interlis.iom_j.itf.ModelUtilities.getIli1AttrList((AbstractClassDef) v);
								Iterator attri = attrv.iterator();
								while (attri.hasNext()) {       
									ch.interlis.ili2c.metamodel.ViewableTransferElement attrObj = (ch.interlis.ili2c.metamodel.ViewableTransferElement) attri.next();                                                                                
									if(attrObj.obj instanceof AttributeDef) {
										AttributeDef attrdefObj = (AttributeDef) attrObj.obj;
										Type type = attrdefObj.getDomainResolvingAliases();
										String attrName = attrdefObj.getContainer().getScopedName(null) + "." + attrdefObj.getName();
										String keyName = attrName.substring(attrName.indexOf(".")+1);
										if (type instanceof EnumerationType) {
											EnumerationType enumType = (EnumerationType) type;
											enumMap.put(keyName, enumType);                                                                                 
										}
									} 
								}
							}
						}
					} 
				}
			}
		}
		return enumMap;
	}


	private HashMap mapEnumerationTypes(HashMap inputEnumerations, HashMap outputEnumerationsions ) {

		HashMap mappings = new HashMap();

		Set keys = outputEnumerations.keySet();
		for (Iterator it = keys.iterator(); it.hasNext();) {
			String key = (String) it.next();

			EnumerationType dm01CtType = (EnumerationType) inputEnumerations.get(key);
			EnumerationType dm01ChType = (EnumerationType) outputEnumerations.get(key);

			HashMap mapping = new HashMap();

			ArrayList evCt = new ArrayList();
			ch.interlis.iom_j.itf.ModelUtilities.buildEnumList(evCt, "", dm01CtType.getConsolidatedEnumeration());

			ArrayList evCh = new ArrayList();
			ch.interlis.iom_j.itf.ModelUtilities.buildEnumList(evCh, "", dm01ChType.getConsolidatedEnumeration());

			Iterator iter = evCt.iterator();
			int k = 0;
			while(iter.hasNext()){
				String Ctcode=(String)iter.next();

				if (evCh.contains(Ctcode)) {
					mapping.put(k, evCh.indexOf(Ctcode));

				} else {
					String[] values = Ctcode.split("\\.");
					String CtcodeTmp = "";
					for (int i = values.length-1; i != 0; i--) {
						for (int j = 0; j < i; j++) {
							CtcodeTmp = CtcodeTmp + "." + values[j];
						}
						CtcodeTmp = CtcodeTmp.substring(1);
						if (evCh.contains(CtcodeTmp)) {
							mapping.put(k, evCh.indexOf(CtcodeTmp));
							break;
						}
					}
				}
				k++;
			}
			mappings.put(key, mapping);
		}
		return mappings;
	}       
}


