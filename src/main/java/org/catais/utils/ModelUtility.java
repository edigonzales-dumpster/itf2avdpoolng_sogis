package org.catais.utils;

/* This file is part of the ili2fme project.
 * For more information, please see <http://www.eisenhutinformatik.ch/interlis/ili2fme/>.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;


import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import ch.interlis.ili2c.metamodel.*;
import ch.interlis.iom.*;
import ch.interlis.iox.*;
import ch.ehi.basics.logging.EhiLogger;


/**
 * @author ce
 * @version $Revision: 1.0 $ $Date: 27.07.2006 $
 */
public class ModelUtility {
	private ModelUtility(){};
	/** @return map<String fmeFeatureTypeName, ViewableWrapper wrapper>
	 */

	public static HashMap getItfTransferViewables(ch.interlis.ili2c.metamodel.TransferDescription td)
	{
		HashMap ret = new HashMap();
		Iterator modeli = td.iterator();
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
						Iterator iter = topic.getViewables().iterator();
						while (iter.hasNext()) {
							Object obj = iter.next();
							if (obj instanceof Viewable) {
								Viewable v = (Viewable) obj;
								if(isPureRefAssoc(v)){
									continue;
								}
								//log.logMessageString("getTransferViewables() leave <"+v+">",IFMELogFile.FME_INFORM);
								String className = v.getScopedName(null);
//								System.out.println(className);
								ViewableWrapper viewableWrapper = new ViewableWrapper(className, v);
								java.util.List attrv = ch.interlis.iom_j.itf.ModelUtilities.getIli1AttrList((AbstractClassDef) v);

								viewableWrapper.setAttrv(attrv);
								ret.put(viewableWrapper.getFmeFeatureType(),viewableWrapper);
								// set geom attr in wrapper
								Iterator attri = v.getAttributes();
								while (attri.hasNext()) {
									Object attrObj = attri.next();
									if (attrObj instanceof AttributeDef) {
										AttributeDef attr =
												(AttributeDef) attrObj;
										Type type =
												Type.findReal(attr.getDomain());
										if (type instanceof PolylineType 
												|| type instanceof SurfaceOrAreaType 
												|| type instanceof CoordType
												){
											viewableWrapper.setGeomAttr4FME(attr);
											break;
										}
									}
								}
								// add helper tables of surface and area attributes
								attri = v.getAttributes();
								while (attri.hasNext()) {
									Object attrObj = attri.next();
									if (attrObj instanceof AttributeDef) {
										AttributeDef attr =
												(AttributeDef) attrObj;
										Type type =
												Type.findReal(attr.getDomain());
										if (type
												instanceof SurfaceOrAreaType) {
											String name =
													v.getContainer().getScopedName(
															null)
															+ "."
															+ v.getName()
															+ "_"
															+ attr.getName();
											ViewableWrapper wrapper =
													new ViewableWrapper(name);
											wrapper.setGeomAttr4FME(attr);
											ArrayList helper_attrv=new ArrayList();
											helper_attrv.add(new ViewableTransferElement(attr));
											wrapper.setAttrv(helper_attrv);
											ret.put(wrapper.getFmeFeatureType(),wrapper);
										}
									}
								}
							}
						}
					}
				}
			}
		}
		return ret;
	}


	public static LinkedHashMap getFeatureTypesFromItfTransferViewables(ch.interlis.ili2c.metamodel.TransferDescription td, LinkedHashMap addAttr, Boolean enumTxt, boolean lv95)
	{
		LinkedHashMap ret = new LinkedHashMap();
		Iterator modeli = td.iterator();
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
						Iterator iter = topic.getViewables().iterator();

						while (iter.hasNext()) {
							Object obj = iter.next();

							if (obj instanceof Viewable) {
								Viewable v = (Viewable) obj;

								if(isPureRefAssoc(v)){
									continue;
								}
								String className = v.getScopedName(null);

								//								 System.out.println("Tabellenname: " + className);

								SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
								typeBuilder.setName(className);
								typeBuilder.setNamespaceURI( "http://www.catais.org" );
								//typeBuilder.setSRS("EPSG:2056");
								typeBuilder.setSRS("EPSG:21781");

								typeBuilder.add("tid", String.class);

								Iterator attri = v.getAttributesAndRoles2();

								while (attri.hasNext()) {
									ch.interlis.ili2c.metamodel.ViewableTransferElement attrObj = (ch.interlis.ili2c.metamodel.ViewableTransferElement) attri.next();
									if(attrObj.obj instanceof AttributeDef) {
										AttributeDef attrdefObj = (AttributeDef) attrObj.obj;
										Type type = attrdefObj.getDomainResolvingAliases();  

										if (type instanceof PolylineType) {
											typeBuilder.add(attrdefObj.getName().toLowerCase(), LineString.class);
											
											if (lv95 == true) {
												typeBuilder.add(attrdefObj.getName().toLowerCase() + "_lv95", LineString.class);
											}

										} else if (type instanceof SurfaceType) {

											String name = attrdefObj.getName();

											// Neuen SimpleFeatureTypeBuilder für
											// Surface Helper-Table erstellen:
											// - tid
											// - foreign key, zB. '_itf_ref_XXXXX (eigentlich egal)
											// - Key (Name) des Builders ist Tabellenname + '_Attributname"
											// Dem 'originalen' (Main-Table) Builder wird
											// eine Polygonklasse hinzugefügt.

											SimpleFeatureTypeBuilder typeBuilderRef = new SimpleFeatureTypeBuilder();
											typeBuilderRef.setName(className+"_"+name);
											typeBuilderRef.setNamespaceURI( "http://www.catais.org" );
											//typeBuilderRef.setSRS("EPSG:2056");
											typeBuilderRef.setSRS("EPSG:21781");

											typeBuilderRef.add("tid", String.class);
											typeBuilderRef.add("_itf_ref", String.class);
											typeBuilderRef.add(name.toLowerCase(), LineString.class);
									
											Iterator it = addAttr.entrySet().iterator();
											while(it.hasNext()) {
												Map.Entry ientry = (Map.Entry)it.next();                
												String attrName = ientry.getKey().toString();
												typeBuilderRef.add(attrName, ientry.getValue().getClass());
											}

											SimpleFeatureType featureTypeRef = typeBuilderRef.buildFeatureType();
											ret.put(className + "_" + name, featureTypeRef);

											typeBuilder.add(name.toLowerCase(), Polygon.class);
											
											if (lv95 == true) {
												typeBuilder.add(name.toLowerCase() + "_lv95", Polygon.class);
											}

										} else if (type instanceof AreaType) {

											String name = attrdefObj.getName();

											// Neuen SimpleFeatureTypeBuilder für
											// Area Helper-Table erstellen. Ähnlich
											// wie oben, nur dass im 'original' Builder 
											// zusätzliche eine Point-Geometrie
											// hinzugefügt werden muss.

											SimpleFeatureTypeBuilder typeBuilderRef = new SimpleFeatureTypeBuilder();
											typeBuilderRef.setName(className+"_"+name);
											typeBuilderRef.setNamespaceURI( "http://www.catais.org" );
											//typeBuilderRef.setSRS("EPSG:2056");
											typeBuilderRef.setSRS("EPSG:21781");

											typeBuilderRef.add("tid", String.class);
											typeBuilderRef.add("_itf_ref", String.class);
											typeBuilderRef.add(name.toLowerCase(), LineString.class);

											if (lv95 == true) {
												typeBuilder.add(name.toLowerCase() + "_lv95", LineString.class);
											}
											
											Iterator it = addAttr.entrySet().iterator();
											while(it.hasNext()) {
												Map.Entry ientry = (Map.Entry)it.next();                
												String attrName = ientry.getKey().toString();
												typeBuilderRef.add(attrName, ientry.getValue().getClass());
											}


											SimpleFeatureType featureTypeRef = typeBuilderRef.buildFeatureType();
											ret.put(className + "_" + name, featureTypeRef);

											typeBuilder.add(name.toLowerCase()+"_point", Point.class);
											typeBuilder.add(name.toLowerCase(), Polygon.class);
											
											if (lv95 == true) {
												typeBuilder.add(name.toLowerCase() + "_lv95", Polygon.class);
											}


										} else if (type instanceof CoordType) {
											typeBuilder.add(attrdefObj.getName().toLowerCase(), Point.class);
											
											if (lv95 == true) {
												typeBuilder.add(attrdefObj.getName().toLowerCase() + "_lv95", Point.class);
											}

										} else if (type instanceof NumericType) {
											typeBuilder.add(attrdefObj.getName().toLowerCase(), Double.class);

										} else if (type instanceof EnumerationType) {
											typeBuilder.add(attrdefObj.getName().toLowerCase(), Integer.class);
											if ( enumTxt == true ) {
												typeBuilder.add(attrdefObj.getName().toLowerCase()+"_txt", String.class);
											}

										} else {
											typeBuilder.add(attrdefObj.getName().toLowerCase(), String.class);
										}
									}
									if(attrObj.obj instanceof RoleDef) {
										RoleDef roledefObj = (RoleDef) attrObj.obj;
										typeBuilder.add(roledefObj.getName().toLowerCase(), String.class);
									}
								}

								Iterator it = addAttr.entrySet().iterator();
								while(it.hasNext()) {
									Map.Entry ientry = (Map.Entry)it.next();                
									String attrName = ientry.getKey().toString();
									typeBuilder.add(attrName, ientry.getValue().getClass());
								}

								SimpleFeatureType featureType = typeBuilder.buildFeatureType();
								
//								System.out.println(featureType.toString());
								
								ret.put(className, featureType);

							}
						}
					}
				}
			}
		}
		return ret;
	}


	public static ArrayList getSQLTableNames(ch.interlis.ili2c.metamodel.TransferDescription td) {

		ArrayList tableNames = new ArrayList();

		Iterator modeli = td.iterator();
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
						Iterator iter = topic.getViewables().iterator();

						while (iter.hasNext()) {
							Object obj = iter.next();

							if (obj instanceof Viewable) {
								Viewable v = (Viewable) obj;

								if(isPureRefAssoc(v)){
									continue;
								}
								String className = v.getScopedName(null);
								String tableName = (className.substring(className.indexOf(".")+1)).replace(".", "_").toLowerCase();
								tableNames.add(tableName);
							}
						}
					}
				}
			}
		}
		return tableNames;
	}


	public static void getTopicsXML(ch.interlis.ili2c.metamodel.TransferDescription td, String schema) {
		try {
			File tempDir = IOUtils.createTempDirectory("qgisxml");
			File sqlFile = new File(tempDir, "topics.xml");

			FileWriter fw = new FileWriter(sqlFile);
			BufferedWriter bw = new BufferedWriter(fw); 

			Iterator modeli = td.iterator();
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

					StringBuffer buf = new StringBuffer("");
					buf.append("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n");
					buf.append("<topics>\n");


					Iterator topici = model.iterator();
					while (topici.hasNext()) {
						Object tObj = topici.next();

						if (tObj instanceof Topic) {
							Topic topic = (Topic) tObj;
							Iterator iter = topic.getViewables().iterator();


							buf.append("  <topic id=\""+topic.getName()+"\" title=\""+topic.getName()+"\" group=\""+topic.getName()+"\">\n");


							while (iter.hasNext()) {
								Object obj = iter.next();

								if (obj instanceof Viewable) {
									Viewable v = (Viewable) obj;

									if(isPureRefAssoc(v)){
										continue;
									}
									String className = v.getScopedName(null);

									System.out.println("Tabellenname: " + className);

									String tableName = (className.substring(className.indexOf(".")+1)).replace(".", "_").toLowerCase();

									int countGeometry = 0;
									ArrayList geomNames = new ArrayList();
									ViewableWrapper viewableWrapper = new ViewableWrapper(className, v);
									java.util.List attrv = ch.interlis.iom_j.itf.ModelUtilities.getIli1AttrList((AbstractClassDef) v);
									Iterator attri = attrv.iterator();
									while (attri.hasNext()) {	
										ch.interlis.ili2c.metamodel.ViewableTransferElement attrObj = (ch.interlis.ili2c.metamodel.ViewableTransferElement) attri.next();										 
										if(attrObj.obj instanceof AttributeDef) {
											AttributeDef attrdefObj = (AttributeDef) attrObj.obj;
											Type type = attrdefObj.getDomainResolvingAliases();
											String attrName = attrdefObj.getName().toLowerCase();
											if (type instanceof PolylineType || type instanceof SurfaceType || type instanceof AreaType || type instanceof CoordType) {
												geomNames.add(attrName);												 
											}
										}
									}

									if (geomNames.size() > 0) {
										StringBuffer bufTable = new StringBuffer("");
										for (int i=0; i<geomNames.size(); i++) {
											bufTable.append("    <table>\n");
											bufTable.append("      <group>"+topic.getName()+"</group>\n");	
											if (geomNames.size() > 1) {
												bufTable.append("      <title>"+v.getName()+" ("+geomNames.get(i)+")</title>\n");
											} else {
												bufTable.append("      <title>"+v.getName()+"</title>\n");
											}
											bufTable.append("      <schema>"+schema+"</schema>\n");
											bufTable.append("      <table>"+tableName+"</table>\n");
											bufTable.append("      <geom>"+geomNames.get(i)+"</geom>\n");
											bufTable.append("      <style></style>\n");
											bufTable.append("    </table>\n");
										}
										buf.append(bufTable.toString());				 
									} else {
										buf.append("    <table>\n");
										buf.append("      <group>"+topic.getName()+"</group>\n");								 										 
										buf.append("      <title>"+v.getName()+"</title>\n");
										buf.append("      <schema>"+schema+"</schema>\n");
										buf.append("      <table>"+tableName+"</table>\n");
										buf.append("      <geom></geom>\n");
										buf.append("      <style></style>\n");
										buf.append("    </table>\n");
									}
								}
							}
							buf.append("  </topic>\n");
						}
					}
					buf.append("</topics>\n");
					bw.write(buf.toString());
				}
			}
			bw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public static void getSQLStatements(ch.interlis.ili2c.metamodel.TransferDescription td, String schema, String adminuser, String selectuser, LinkedHashMap addAttr, Boolean enumTxt, String epsg, boolean lv95) {
		String grantUser = adminuser;
		String selectUser = selectuser;
		

		try {
			Map CLASS_MAPPINGS = new HashMap();

			CLASS_MAPPINGS.put(String.class, "VARCHAR");
			CLASS_MAPPINGS.put(Boolean.class, "BOOLEAN");
			CLASS_MAPPINGS.put(Integer.class, "INTEGER");
			CLASS_MAPPINGS.put(Long.class, "BIGINT");
			CLASS_MAPPINGS.put(Float.class, "REAL");
			CLASS_MAPPINGS.put(Double.class, "DOUBLE PRECISION");
			CLASS_MAPPINGS.put(BigDecimal.class, "DECIMAL");
			CLASS_MAPPINGS.put(java.sql.Date.class, "DATE");
			CLASS_MAPPINGS.put(java.util.Date.class, "DATE");
			CLASS_MAPPINGS.put(java.sql.Time.class, "TIME");
			CLASS_MAPPINGS.put(java.sql.Timestamp.class, "TIMESTAMP");				

			Map GEOM_TYPE_MAP = new HashMap();

			GEOM_TYPE_MAP.put("GEOMETRY", Geometry.class);
			GEOM_TYPE_MAP.put("POINT", Point.class);
			GEOM_TYPE_MAP.put("LINESTRING", LineString.class);
			GEOM_TYPE_MAP.put("POLYGON", Polygon.class);
			GEOM_TYPE_MAP.put("MULTIPOINT", MultiPoint.class);
			GEOM_TYPE_MAP.put("MULTILINESTRING", MultiLineString.class);
			GEOM_TYPE_MAP.put("MULTIPOLYGON", MultiPolygon.class);
			GEOM_TYPE_MAP.put("GEOMETRYCOLLECTION", GeometryCollection.class);

			Map GEOM_CLASS_MAPPINGS = new HashMap();

			Set keys = GEOM_TYPE_MAP.keySet();

			for (Iterator it = keys.iterator(); it.hasNext();) {
				String name = (String) it.next();
				Class geomClass = (Class) GEOM_TYPE_MAP.get(name);
				GEOM_CLASS_MAPPINGS.put(geomClass, name);
			}

			File tempDir = IOUtils.createTempDirectory("pgcreatestatements");
			File sqlFile = new File(tempDir, "create.sql");

			FileWriter fw = new FileWriter(sqlFile);
			BufferedWriter bw = new BufferedWriter(fw); 
			StringBuffer buf = new StringBuffer("");

			buf.append("-------------- Create schema --------------\n\n");
			buf.append("CREATE SCHEMA " + schema + " AUTHORIZATION " + grantUser + ";\n");
			buf.append("GRANT ALL ON SCHEMA " + schema + " TO " + grantUser + ";\n");
			buf.append("GRANT USAGE ON SCHEMA " + schema + " TO " + selectUser + ";\n\n");
			bw.write(buf.toString());

			Iterator modeli = td.iterator();
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
							Iterator iter = topic.getViewables().iterator();


							while (iter.hasNext()) {
								Object obj = iter.next();

								if (obj instanceof Viewable) {
									Viewable v = (Viewable) obj;

									if(isPureRefAssoc(v)){
										continue;
									}
									String className = v.getScopedName(null);

									System.out.println("Tabellenname: " + className);

									// Array für btree Index
									ArrayList btree_idx = new ArrayList();

									String tableName = (className.substring(className.indexOf(".")+1)).replace(".", "_").toLowerCase();

									buf = new StringBuffer("");
									buf.append("-------------- New Table --------------\n\n");

									buf.append("CREATE TABLE " + schema + "." + tableName);
									buf.append("\n");
									buf.append("(");
									buf.append("\n ");
									buf.append("ogc_fid SERIAL PRIMARY KEY, \n ");
									buf.append("tid VARCHAR, \n");

									btree_idx.add("ogc_fid");
									btree_idx.add("tid");

									// Arrays werden gebraucht für Index
									// und geometry columns inserts.
									ArrayList geomName = new ArrayList();
									ArrayList geomType = new ArrayList();
									String srid = epsg;

									ViewableWrapper viewableWrapper = new ViewableWrapper(className, v);
									java.util.List attrv = ch.interlis.iom_j.itf.ModelUtilities.getIli1AttrList((AbstractClassDef) v);

									Iterator attri = attrv.iterator();

									while (attri.hasNext()) {	
										ch.interlis.ili2c.metamodel.ViewableTransferElement attrObj = (ch.interlis.ili2c.metamodel.ViewableTransferElement) attri.next();										 

										if(attrObj.obj instanceof AttributeDef) {
											AttributeDef attrdefObj = (AttributeDef) attrObj.obj;
											Type type = attrdefObj.getDomainResolvingAliases();  

											String attrName = attrdefObj.getName().toLowerCase();

											buf.append(" ");
											buf.append(attrName + " ");

											String typeName = null;
											if (type instanceof PolylineType) {
												geomType.add("LINESTRING");
												geomName.add(attrName);
												buf.append("GEOMETRY,\n"); 
												
												if (lv95 == true) {
													buf.append(" ");
													buf.append(attrName + "_lv95 ");
													
													geomType.add("LINESTRING");
													geomName.add(attrName + "_lv95");
													buf.append("GEOMETRY,\n"); 
												}

											} else if (type instanceof SurfaceType) {												 
												// Die Helper-Tables Statements werden
												// nicht erzeugt, sonst analog der Feature-
												// Types hier schreiben.

												geomType.add("POLYGON");
												geomName.add(attrName);
												buf.append("GEOMETRY,\n"); 
												
												if (lv95 == true) {
													buf.append(" ");
													buf.append(attrName + "_lv95 ");

													geomType.add("POLYGON");
													geomName.add(attrName + "_lv95");
													buf.append("GEOMETRY,\n"); 
												}												

											} else if (type instanceof AreaType) {
												// Die Helper-Tables Statements werden
												// nicht erzeugt, sonst analog der Feature-
												// Types hier schreiben.

												geomType.add("POLYGON");
												geomName.add(attrName);
												buf.append("GEOMETRY,\n"); 
												
												if (lv95 == true) {
													buf.append(" ");
													buf.append(attrName + "_lv95 ");

													geomType.add("POLYGON");
													geomName.add(attrName + "_lv95");
													buf.append("GEOMETRY,\n"); 
												}

											} else if (type instanceof CoordType) {
												geomType.add("POINT");
												geomName.add(attrName);
												buf.append("GEOMETRY,\n");
												
												if (lv95 == true) {
													buf.append(" ");
													buf.append(attrName + "_lv95 ");

													geomType.add("POINT");
													geomName.add(attrName + "_lv95");
													buf.append("GEOMETRY,\n"); 
												}												

											} else if (type instanceof NumericType) {
												buf.append("DOUBLE PRECISION,\n");

											} else if (type instanceof EnumerationType) {
												buf.append("INTEGER,\n");

												// Append also the text of the enum stuff.
												if (enumTxt == true) {
													buf.append(" ");
													buf.append(attrName + "_txt ");
													buf.append("VARCHAR,\n");
												}

											} else {
												buf.append("VARCHAR,\n");		
											}
										}
										if(attrObj.obj instanceof RoleDef) {
											RoleDef roledefObj = (RoleDef) attrObj.obj;
											String roleDefName = roledefObj.getName().toLowerCase();
											buf.append(" "+roleDefName+" ");
											buf.append("VARCHAR,\n");	

											btree_idx.add(roleDefName);
										}
									}

									// Die zusätzlichen Attribute werden
									// abgearbeitet.
									Iterator it = addAttr.entrySet().iterator();
									while(it.hasNext()) {
										Map.Entry ientry = (Map.Entry)it.next();                
										String attrName = ientry.getKey().toString();
										buf.append(" "+attrName+" ");
										buf.append((String) CLASS_MAPPINGS.get(ientry.getValue().getClass())+",\n");

										btree_idx.add(attrName);
									}

									buf.deleteCharAt(buf.length()-2);
									buf.append(")\n");
									buf.append("WITH (OIDS=FALSE);\n");
									buf.append("ALTER TABLE " + schema + "." + tableName + " OWNER TO " + grantUser + ";\n");
									buf.append("GRANT ALL ON " + schema + "." + tableName + " TO " + grantUser + ";\n");
									buf.append("GRANT SELECT ON " + schema + "." + tableName + " TO " + selectUser + ";\n\n");

									// Die NICHT-Geomtrie Index schreiben
									for(int i=0; i<btree_idx.size(); i++) {
										buf.append("CREATE INDEX idx_" + tableName + "_" + btree_idx.get(i)  + "\n");
										buf.append("  ON "+schema+"."+tableName+"\n");
										buf.append("  USING btree\n");
										buf.append("  ("+btree_idx.get(i)+");\n\n");	

									}


									// Geometrie Index schreiben und in 
									// geometry_columns schreiben
									for(int i=0; i<geomName.size(); i++) {

										buf.append("CREATE INDEX idx_" + tableName + "_" + geomName.get(i)  + "\n");
										buf.append("  ON "+schema+"."+tableName+"\n");
										buf.append("  USING gist\n");
										buf.append("  ("+geomName.get(i)+");\n\n");	

										buf.append("INSERT INTO geometry_columns VALUES ('\"', '" + schema + "', '" + tableName + "', '" + geomName.get(i) + "', 2, '" + srid + "', '" + ((String)geomType.get(i)).toUpperCase() +"');\n\n");	 
									}
									//									 System.out.println(buf.toString());
									bw.write(buf.toString());
								}
							}
						}
					}
				}
			}
			bw.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}



	public static boolean isPureRefAssoc(Viewable v) {
		if(!(v instanceof AssociationDef)){
			return false;
		}
		AssociationDef assoc=(AssociationDef)v;
		// embedded and no attributes/embedded links?
		if(assoc.isLightweight() && 
				!assoc.getAttributes().hasNext()
				&& !assoc.getLightweightAssociations().iterator().hasNext()
				) {
			return true;
		}
		return false;
	}

}