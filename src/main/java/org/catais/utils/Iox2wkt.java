package org.catais.utils;


import java.io.IOException;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.geotools.data.FeatureStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureCollections;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.Filter;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.operation.linemerge.LineMerger;
import com.vividsolutions.jts.precision.SimpleGeometryPrecisionReducer;

import ch.interlis.ili2c.metamodel.*;
import ch.interlis.iom.*;
import ch.interlis.iox.*;
import ch.interlis.iom_j.itf.ItfReader;
import ch.interlis.iox_j.jts.Iox2jts;
import ch.interlis.iox_j.jts.Iox2jtsException;
import ch.interlis.iox_j.jts.Jts2iox;

/**
 * Methoden zur Umwandlung von Iom-Objekten nach JTS-Geometrien.
 *
 * @author Stefan Ziegler
 * @version 0.1
 */

public class Iox2wkt {
	private static Logger logger = Logger.getLogger( Iox2wkt.class );

	private static final CoordinateList coords = new CoordinateList();

	private static PrecisionModel pm = new PrecisionModel(1000);
	private static SimpleGeometryPrecisionReducer pr = new SimpleGeometryPrecisionReducer(pm);


	public Iox2wkt() {

	}

	/**
	 * Wandelt ein IOM Polylineobjekt in Liniensegmente (Linestrings mit 2 resp. 3 Punkten) um.
	 * Kreisbögen werden einfach segmentiert. Dh. es wird von Bogenanfang über Bogenpunkt
	 * zum Bogenende verbunden. Es werden
	 *
	 * @param polylineObj IOM Polylineobjekt
	 * @return Liste mit Geometrien
	 */

	public static ArrayList polyline2segments(IomObject polylineObj) {

		coords.clear();

		ArrayList lines = new ArrayList();
		Coordinate coord_tmp = new Coordinate();
		Coordinate curve_tmp = null;

		boolean clipped = polylineObj.getobjectconsistency()==IomConstants.IOM_INCOMPLETE;
		for(int sequencei=0; sequencei<polylineObj.getattrvaluecount("sequence"); sequencei++) {
			if(!clipped && sequencei>0) {
				throw new IllegalArgumentException();
			}

			IomObject sequence=polylineObj.getattrobj("sequence", sequencei);
			for(int segmenti=0; segmenti<sequence.getattrvaluecount("segment"); segmenti++) {
				IomObject segment=sequence.getattrobj("segment", segmenti);
				if(segment.getobjecttag().equals("COORD")) {

					if(curve_tmp != null) {
						coords.clear();
						coords.add(curve_tmp, false);
						curve_tmp = null;
					}

					String c1=segment.getattrvalue("C1");
					String c2=segment.getattrvalue("C2");
					//String c3=segment.getattrvalue("C3");

					Coordinate coord = new Coordinate(Double.valueOf(c1), Double.valueOf(c2));

					// Erste Koordinate muss hinzugefügt werden.
					// Ist immer vom Typ COORD.
					if(coords.size() == 0 || coords.size() == 1) {
						coords.add(coord, false);
					}

					if(coords.size() == 2) {
						Geometry line = new GeometryFactory().createLineString(coords.toCoordinateArray());
						//System.out.println(line.toString());
						lines.add(pr.reduce(line));
						coords.clear();
						coords.add(coord, false);
					}

					coord_tmp = coord;
				} else if (segment.getobjecttag().equals("ARC")) {
					String a1=segment.getattrvalue("A1");
					String a2=segment.getattrvalue("A2");
					String c1=segment.getattrvalue("C1");
					String c2=segment.getattrvalue("C2");
					//String c3=segment.getattrvalue("C3");

					Coordinate ptStart = coord_tmp;
					Coordinate ptArc = new Coordinate(Double.valueOf(a1), Double.valueOf(a2));
					Coordinate ptEnd = new Coordinate(Double.valueOf(c1), Double.valueOf(c2));

					CoordinateList curve = new CoordinateList();
					curve.add(ptStart);
					curve.add(ptArc);
					curve.add(ptEnd);

					Geometry line = new GeometryFactory().createLineString(curve.toCoordinateArray());
					//System.out.println(line.toString());
					lines.add(pr.reduce(line));
					curve.clear();

					coord_tmp = ptEnd;
					curve_tmp = ptEnd;
				} else {
					// custom line form is not supported
				}      
			}
		}
		return lines;
	}



	/**
	 * Wandelt ein IOM Polylineobjekt in eine JTS-Geometrie um. Kreisbögen werden segmentiert.
	 * Durch die Angabe des korrekten maxOverlap wird vermieden, dass Zentroidpunkte ausserhalb
	 * des Polygons zu liegen kommt. Dabei wird aus dem maxOverlap resp. der maximalen Pfeilhöhe
	 * der Segmentierungswinkel berechnet.d
	 *
	 * @param polylineObj IOM Polylineobjekt
	 * @param maxOverlaps maximal erlaubter Overlap
	 * @return JTS Linestring
	 */

	/**
	 * @param polylineObj
	 * @param maxOverlaps
	 * @return
	 */
	
	public static Geometry polyline2jts(IomObject polylineObj, double maxOverlaps) {
		
		return polyline2jts(polylineObj, maxOverlaps, null, null, null, null);
	}
	
	
	public static Geometry polyline2jts(IomObject polylineObj, double maxOverlaps, FeatureStore<SimpleFeatureType, SimpleFeature>  store, String className, Integer gem_bfs, Integer los) 
	{	
		GeometryFactory gf = new GeometryFactory();
		
		LineString[] linestrings = null;
		ArrayList lines = new ArrayList();

		FeatureCollection collection = FeatureCollections.newCollection();
		
		boolean clipped = polylineObj.getobjectconsistency()==IomConstants.IOM_INCOMPLETE;
		for(int sequencei=0; sequencei<polylineObj.getattrvaluecount("sequence"); sequencei++) {
			if(!clipped && sequencei>0) {
				throw new IllegalArgumentException();
			}
			
			Coordinate pt0 = null;

			IomObject sequence=polylineObj.getattrobj("sequence", sequencei);
			for(int segmenti=0; segmenti<sequence.getattrvaluecount("segment"); segmenti++) {
				IomObject segment=sequence.getattrobj("segment", segmenti);
				
//				logger.debug("segmenti " + segmenti + ": " + segment.toString());
	
				if(segment.getobjecttag().equals("COORD")) {                                    

					String c1=segment.getattrvalue("C1");
					String c2=segment.getattrvalue("C2");
										
					Coordinate coord = new Coordinate(Double.valueOf(c1), Double.valueOf(c2));
									
					if ( pt0 != null )
					{
						LineSegment linesegment = new LineSegment(pt0, coord);
						lines.add(linesegment.toGeometry(gf));
//						logger.debug(linesegment.toString());
					}
					
					pt0 = new Coordinate(coord);

				} else if (segment.getobjecttag().equals("ARC")) {
					String a1=segment.getattrvalue("A1");
					String a2=segment.getattrvalue("A2");
					String c1=segment.getattrvalue("C1");
					String c2=segment.getattrvalue("C2");

					Coordinate ptStart = new Coordinate(pt0);
					Coordinate ptArc = new Coordinate(Double.valueOf(a1), Double.valueOf(a2));
					Coordinate ptEnd = new Coordinate(Double.valueOf(c1), Double.valueOf(c2));

					LineSegment linesegment = new LineSegment(ptStart, ptEnd);
					pt0 = new Coordinate(ptEnd);

					// Liniensegment normalisieren aka Kreisbogen 'normalisieren'.
					linesegment.normalize();
					ptStart = new Coordinate(linesegment.p0);
					ptEnd = new Coordinate(linesegment.p1);
					
					LineString line = null;
					
					// Prüfen ob Kreisbogen in einer anderen Gemeinde bereits existiert.
					// Falls kein store übergeben wird, wird wie bis anhin ohne Berücksichtigung anderer Kreisbögen segmentiert.
					if (store != null) {
						try {		
							//logger.debug("arc helper...");	
							// Im blödsten Fall müsste man noch prüfen, ob es wirklich der gleich Bogen ist (konkav <-> konvex).
							org.opengis.filter.Filter filter = CQL.toFilter("NOT (gem_bfs = '" + gem_bfs + "' AND los = '" + los + "') AND classname = '" + className + "' AND EQUALS(ptstart, POINT(" + ptStart.x + " " + ptStart.y + ")) AND EQUALS(ptend, POINT(" + ptEnd.x + " " + ptEnd.y + "))");
							//logger.debug(filter);
							FeatureCollection<SimpleFeatureType, SimpleFeature> fc = store.getFeatures(filter);
							if (fc.size() == 0) {
								SimpleFeatureType ft = fc.getSchema();
								SimpleFeatureBuilder fb = new SimpleFeatureBuilder( ft );
								fb.set("classname", className);
								fb.set("gem_bfs", gem_bfs);
								fb.set("los", los);
								fb.set("ptstart", new GeometryFactory().createPoint(ptStart));
								fb.set("ptarc", new GeometryFactory().createPoint(ptArc));
								fb.set("ptend", new GeometryFactory().createPoint(ptEnd));
								line = interpolateArc(ptStart, ptArc, ptEnd, maxOverlaps);
								fb.set("arc", line);

								SimpleFeature feature = fb.buildFeature(null);

								collection.add(feature);
							} else {
								Iterator it = fc.iterator();
								while(it.hasNext()) {
									line = (LineString) ((SimpleFeature) it.next()).getAttribute("arc");
									logger.debug(line);
									break;
								}
								fc.close(it);
							}	
						} catch (CQLException e) {
							e.printStackTrace();
							logger.error(e.getMessage());
						} catch (IOException e) {
							e.printStackTrace();
							logger.error(e.getMessage());
						}
					} else {
						line = interpolateArc(ptStart, ptArc, ptEnd, maxOverlaps);
					}
					lines.add(line);
				} else {
					logger.error("custom line form is not supported");
				}      
			}
		}
		
		linestrings = (LineString[]) lines.toArray(new LineString[0]);
		MultiLineString multilinestring = new MultiLineString(linestrings, gf);
		
		LineMerger merger = new LineMerger();
		merger.add(multilinestring);
		Collection coll = merger.getMergedLineStrings();
		LineString line = (LineString) coll.toArray()[0];
//		logger.debug(line.toString());
		
		// Allenfalls neue Kreisbogen speichern.
		if (collection.size() != 0) {
			try {	
				//logger.debug(collection.size());
				store.addFeatures(collection);
			} catch (IOException e) {
				e.printStackTrace();
				logger.error(e.getMessage());
			}
		}

		
		
//		logger.debug("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
		
		return line;
	}



	/**
	 * Segmentiert ein Kreisbogenelement. Durch die Angabe des korrekten maxOverlap wird vermieden, dass Zentroidpunkte ausserhalb
	 * des Polygons zu liegen kommt. Dabei wird aus dem maxOverlap resp. der maximalen Pfeilhöhe
	 * der Segmentierungswinkel berechnet.
	 *
	 * @param ptStart Bogenanfang
	 * @param ptArc Bogenpunkt
	 * @param ptEnd Bogenende
	 * @param maxOverlaps maximal erlaubter Overlap
	 */
	private static LineString interpolateArc(Coordinate ptStart, Coordinate ptArc, Coordinate ptEnd, double maxOverlaps) {

//		logger.setLevel(Level.DEBUG);
//		logger.debug("Start: " + ptStart.toString());
//		logger.debug("Arc: " + ptArc.toString());
//		logger.debug("End: " + ptEnd.toString());
		
		
		CoordinateList mycoords = new CoordinateList();
		mycoords.add(ptStart);
		
		double arcIncr = 1;
		if(maxOverlaps < 0.00001) {
			maxOverlaps = 0.002;
		}
		
		// TEMPORARY:
		maxOverlaps = 0.002;

		LineSegment segment = new LineSegment( ptStart, ptEnd );
		double dist = segment.distancePerpendicular( ptArc );
		
		// Abbruchkriterium Handgelenkt mal Pi...
		if ( dist < maxOverlaps )
		{
			mycoords.add(ptEnd);
			LineString line = new GeometryFactory().createLineString(mycoords.toCoordinateArray());
			return line;
		}
		
		Coordinate center = getArcCenter(ptStart, ptArc, ptEnd);

		double cx = center.x; double cy = center.y;
		double px = ptArc.x; double py = ptArc.y;
		// Radius noch auf Meter runden. Ob sinnvoll oder nicht, wird sich zeigen
		// NEIN, keine schlaue Idee: Zentroidpunkte liegen plötzlich nicht mehr 
		// innerhalb des Polygons -> nicht runden.
		double r = Math.sqrt((cx-px)*(cx-px)+(cy-py)*(cy-py));
		//logger.debug("radius: " + r);

		double myAlpha = 2.0*Math.acos(1.0-maxOverlaps/r);

		if (myAlpha < arcIncr)  {
			arcIncr = myAlpha;
		}

		double a1 = Math.atan2(ptStart.y - center.y, ptStart.x - center.x);
		double a2 = Math.atan2(ptArc.y - center.y, ptArc.x - center.x);
		double a3 = Math.atan2(ptEnd.y - center.y, ptEnd.x - center.x);

		double sweep;

		// Clockwise
		if(a1 > a2 && a2 > a3) {
			sweep = a3 - a1;
		}
		// Counter-clockwise
		else if(a1 < a2 && a2 < a3) {
			sweep = a3 - a1;
		}
		// Clockwise, wrap
		else if((a1 < a2 && a1 > a3) || (a2 < a3 && a1 > a3)) {
			sweep = a3 - a1 + 2*Math.PI;
		}
		// Counter-clockwise, wrap
		else if((a1 > a2 && a1 < a3) || (a2 > a3 && a1 < a3)) {
			sweep = a3 - a1 - 2*Math.PI;
		}
		else {
			sweep = 0.0;
		}

		double ptcount = Math.ceil(Math.abs(sweep/arcIncr));

		if(sweep < 0) arcIncr *= -1.0;

		double angle = a1;

		for(int i = 0; i < ptcount - 1; i++) {
			angle += arcIncr;

			if(arcIncr > 0.0 && angle > Math.PI) angle -= 2*Math.PI;
			if(arcIncr < 0.0 && angle < -1*Math.PI) angle -= 2*Math.PI;

			double x = cx + r*Math.cos(angle);
			double y = cy + r*Math.sin(angle);

			Coordinate coord =  new Coordinate(x, y);
			mycoords.add(coord, false);

			// Den Arcpoint wollen wir nicht mehr im Linesegment!
			/*
			if((angle < a2) && ((angle + arcIncr) > a2)) {
				coords.add(ptArc, false);
			}
			if((angle > a2) && ((angle + arcIncr) < a2)) {
				coords.add(ptArc, false);
			}
			*/
		}
		mycoords.add(ptEnd, false);    
		LineString line = new GeometryFactory().createLineString(mycoords.toCoordinateArray());
		return line;
	}


	/**
	 * Berechnet Kreisbogenzentrum.
	 *
	 * @param ptStart Bogenanfang
	 * @param ptArc Bogenpunkt
	 * @param ptEnd Bogenende
	 * @return Kreisbogenzentrum
	 */
	private static Coordinate getArcCenter(Coordinate ptStart, Coordinate ptArc, Coordinate ptEnd) {
		double bx = ptStart.x;
		double by = ptStart.y;
		double cx = ptArc.x;
		double cy = ptArc.y;
		double dx = ptEnd.x;
		double dy = ptEnd.y;
		double temp, bc, cd, det, x, y;

		temp = cx * cx + cy * cy;
		bc = (bx * bx + by * by - temp) / 2.0;
		cd = (temp - dx * dx - dy * dy) / 2.0;
		det = (bx - cx) * (cy - dy) - (cx - dx) * (by - cy);

		det = 1 / det;

		x = (bc * (cy - dy) - cd * (by - cy)) * det;
		y = ((bx - cx) * cd - (cx - dx) * bc) * det;

		return new Coordinate(x, y);            
	}

	
	/**
	 * Wandelt ein IOM-Punktelement in ein WKT-String um.
	 * @param pointObj IOM-Punktelement
	 * @return WKT-Punktgeometrie
	 */
	public static String point2wkt(IomObject pointObj) {
		String point_wkt = null;

		String c1 = pointObj.getattrvalue("C1");
		String c2 = pointObj.getattrvalue("C2");

		point_wkt = "POINT(" + c1 + " " + c2 + ")";

		return point_wkt;
	}


	/**
	 * Wandelt ein IOM-Polylineobjekt in ein WKT-String um.
	 * Kreisbogen werden unterstützt.
	 *
	 * @param polylineObj IOM-Polylineobjekt
	 * @return WKT-Linestring oder WKT-Compoundcurve
	 */
	public static String polyline2wkt(IomObject polylineObj) {

		String polyline_wkt = null;

		boolean clipped = polylineObj.getobjectconsistency()==IomConstants.IOM_INCOMPLETE;
		for(int sequencei=0; sequencei<polylineObj.getattrvaluecount("sequence"); sequencei++) {
			if(!clipped && sequencei>0) {
				throw new IllegalArgumentException();
			}
			Boolean curved = false;
			Boolean curves = false;
			StringBuffer line = new StringBuffer("(");
			String c1_tmp = null;
			String c2_tmp = null;

			IomObject sequence=polylineObj.getattrobj("sequence", sequencei);
			for(int segmenti=0; segmenti<sequence.getattrvaluecount("segment"); segmenti++) {
				IomObject segment=sequence.getattrobj("segment", segmenti);
				if(segment.getobjecttag().equals("COORD")) {                                    

					if(curved) {
						line.append(",(");
						line.append(c1_tmp + " " + c2_tmp);
						line.append(",");
						curved = false;
					}

					String c1=segment.getattrvalue("C1");
					String c2=segment.getattrvalue("C2");
					String c3=segment.getattrvalue("C3");

					line.append(c1 + " " + c2);
					line.append(",");

					c1_tmp = c1;
					c2_tmp = c2;

				} else if (segment.getobjecttag().equals("ARC")) {

					// Falls ein Circuarstring vorangegangen ist,
					// muss noch ein Komma gesetzte werden.
					if(curved) {
						line.append(",");
					}

					// Liniensegment abschliessen, aber nur
					// falls kein Circularstring vorangegangen
					// ist.
					if(segmenti != 0 && curved == false) {
						line.deleteCharAt(line.length()-1);
						line.append("),");
						//System.out.println(.toString());
					}

					// Flags für Kreisbogen setzen
					curved = true;
					curves = true;

					String c1=segment.getattrvalue("C1");
					String c2=segment.getattrvalue("C2");
					String c3=segment.getattrvalue("C3");
					String a1=segment.getattrvalue("A1");
					String a2=segment.getattrvalue("A2");

					String curve = "CIRCULARSTRING(" + c1_tmp + " " + c2_tmp + "," + a1 + " " + a2 + "," + c1 + " " + c2 + ")";

					line.append(curve);

					c1_tmp = c1;
					c2_tmp = c2;

				} else {
					// custom line form
				}

				// Falls keine Kreisbögen muss das
				// Liniensegment hier geschlossen werden.                                      
				if(segmenti == (sequence.getattrvaluecount("segment")-1) && segmenti != 0) {
					line.deleteCharAt(line.length()-1);
					line.append(")");
				}
			}
			// Falls keine Kreisbögen vorhanden sind,
			// gehts als Linestring durch, sonst
			// als Compoundcurve.
			if(curves) {
				line.insert(0, "COMPOUNDCURVE(");
				line.append(")");
			} else {
				line.insert(0, "LINESTRING");
				line.append("");
			}

			polyline_wkt = line.toString();
			//System.out.println(line.toString());

		}
		return polyline_wkt;
	}
	
	/**
	 * Wandelt ein IOM Polylineobjekt in eine JTS-Geometrie um. Kreisbögen werden nicht segmentiert.
	 * Es wird vom Bogenanfang zum Bogenende direkt verbunden.
	 *
	 * @param polylineObj IOM Polylineobjekt
	 * @param maxOverlaps maximal erlaubter Overlap
	 * @return JTS Linestring
	 */    
	public static Geometry polyline2curvelessjts(IomObject polylineObj, double maxOverlaps) {

		coords.clear();

		boolean clipped = polylineObj.getobjectconsistency()==IomConstants.IOM_INCOMPLETE;
		for(int sequencei=0; sequencei<polylineObj.getattrvaluecount("sequence"); sequencei++) {
			if(!clipped && sequencei>0) {
				throw new IllegalArgumentException();
			}

			IomObject sequence=polylineObj.getattrobj("sequence", sequencei);
			for(int segmenti=0; segmenti<sequence.getattrvaluecount("segment"); segmenti++) {
				IomObject segment=sequence.getattrobj("segment", segmenti);
				if(segment.getobjecttag().equals("COORD")) {                                    

					String c1=segment.getattrvalue("C1");
					String c2=segment.getattrvalue("C2");
					//String c3=segment.getattrvalue("C3");

					Coordinate coord = new Coordinate(Double.valueOf(c1), Double.valueOf(c2));
					coords.add(coord, false);

				} else if (segment.getobjecttag().equals("ARC")) {
					//String a1=segment.getattrvalue("A1");
					//String a2=segment.getattrvalue("A2");
					String c1=segment.getattrvalue("C1");
					String c2=segment.getattrvalue("C2");
					//String c3=segment.getattrvalue("C3");

					Coordinate coord = new Coordinate(Double.valueOf(c1), Double.valueOf(c2));
					coords.add(coord, false);

					//Coordinate ptStart = coords.getCoordinate(coords.size()-1);
					//Coordinate ptArc = new Coordinate(Double.valueOf(a1), Double.valueOf(a2));
					//Coordinate ptEnd = new Coordinate(Double.valueOf(c1), Double.valueOf(c2));

					//interpolateArc(ptStart, ptArc, ptEnd, maxOverlaps);

				} else {
					// custom line form is not supported
				}      
			}
		}

		Geometry line = new GeometryFactory().createLineString(coords.toCoordinateArray());
		//System.out.println(line);
		return line;
	}      


}

