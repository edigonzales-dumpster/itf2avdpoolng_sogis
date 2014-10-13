package org.catais.geobau;

import java.io.RandomAccessFile;
import java.io.InputStreamReader;
import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;


/**
 * Classe representant une table fichier DXF.
 * @version 0.2 (2006-10-19)
 * add multi-geometry export
 * add attribute tests an ability to export ANY jump layer
 * add ability to export holes in a separate layer or not
 */
public class DxfENTITY {
	private static Logger logger = Logger.getLogger(DxfENTITY.class);

	public final static DxfGroup LINE = new DxfGroup(0, "LINE");
	public final static DxfGroup POINT = new DxfGroup(0, "POINT");
	public final static DxfGroup CIRCLE = new DxfGroup(0, "CIRCLE");
	public final static DxfGroup ARC = new DxfGroup(0, "ARC");
	public final static DxfGroup TRACE = new DxfGroup(0, "TRACE");
	public final static DxfGroup SOLID = new DxfGroup(0, "SOLID");
	public final static DxfGroup TEXT = new DxfGroup(0, "TEXT");
	public final static DxfGroup SHAPE = new DxfGroup(0, "SHAPE");
	public final static DxfGroup BLOCK = new DxfGroup(0, "BLOCK");
	public final static DxfGroup ENDBLK = new DxfGroup(0, "ENDBLK");
	public final static DxfGroup INSERT = new DxfGroup(0, "INSERT");
	public final static DxfGroup ATTDEF = new DxfGroup(0, "ATTDEF");
	public final static DxfGroup ATTRIB = new DxfGroup(0, "ATTRIB");
	public final static DxfGroup POLYLINE = new DxfGroup(0, "POLYLINE");
	public final static DxfGroup VERTEX = new DxfGroup(0, "VERTEX");
	public final static DxfGroup SEQEND = new DxfGroup(0, "SEQEND");
	public final static DxfGroup _3DFACE = new DxfGroup(0, "3DFACE");
	public final static DxfGroup VIEWPORT = new DxfGroup(0, "VIEWPORT");
	public final static DxfGroup DIMENSION = new DxfGroup(0, "DIMENSION");
	public final static PrecisionModel DPM = new PrecisionModel(); 
	public static int precision = 3;

	private String layerName = "DEFAULT";
	private String lineType = null;
	private float elevation = 0f;
	private float thickness = 0f;
	private int colorNumber = 256;
	private int space = 0;
	private double[] extrusionDirection = null;
	private int flags = 0;

	public String getLayerName() {return layerName;}

	public void setLayerName(String layerName) {
		this.layerName = layerName;
	}

	public DxfENTITY(String layerName) {
		this.layerName = layerName;
	}

	public static String feature2Dxf(SimpleFeature feature, String layerName, boolean suffix) throws Exception {
		logger.setLevel(Level.INFO);

		Geometry g = (Geometry) feature.getDefaultGeometry();
		if (g.getGeometryType().equals("Point")) {
			return point2Dxf(feature, layerName);
		}
		else if (g.getGeometryType().equals("LineString")) {
			return lineString2Dxf(feature, layerName);
		}
		else if (g.getGeometryType().equals("Polygon")) {
			return polygon2Dxf(feature, layerName, suffix);
		}
		else {
			logger.warn("Geometrycollection cannot be processed.");
			return null;
		}
	}


	public static String point2Dxf(SimpleFeature feature, String layerName) throws Exception {
		logger.setLevel(Level.INFO);
		StringBuffer sb = null;

		// stefan ziegler / 2008-07-25
		// Ergaenzung fuer Blockelemente (Symbole fuer AV-Punktelemente)
		
		
		if(feature.getProperty("block") != null) {
			sb = new StringBuffer(DxfGroup.toString(0, "INSERT"));			
			sb.append(DxfGroup.toString(2, (String) feature.getAttribute("block").toString()));
			sb.append(DxfGroup.toString(8, layerName));
			Coordinate coord = ((Point)feature.getDefaultGeometry()).getCoordinate();
			sb.append(DxfGroup.toString(10, coord.x, precision));
			sb.append(DxfGroup.toString(20, coord.y, precision));

			// Stefan Ziegler / 2009-12-08
			// Hoehe wird als "_Z"-Attribut uebergeben, da JTS/JUMP 2D.
			if(feature.getAttribute("_z") != null) {
				double h = (Double) feature.getAttribute("_z");
				if((Double) feature.getAttribute("_z") > 0) {
					sb.append(DxfGroup.toString(30, h, precision));
				}
			}
			sb.append(DxfGroup.toString(50, "0.0"));
			sb.append(DxfGroup.toString(41, "0.5"));
			sb.append(DxfGroup.toString(42, "0.5"));
			sb.append(DxfGroup.toString(43, "0.5"));
		} else if(feature.getProperty("text") != null) {
			sb = new StringBuffer(DxfGroup.toString(0, "TEXT"));
			sb.append(DxfGroup.toString(1, (String) feature.getAttribute("text")));
			sb.append(DxfGroup.toString(40, (Double) feature.getAttribute("text_size")));
			//sb.append(DxfGroup.toString(7, "STANDARD"));
			sb.append(DxfGroup.toString(8, layerName));
			//sb.append(DxfGroup.toString(6, "CONTINOUS"));
			//sb.append(DxfGroup.toString(62, "7"));
			Coordinate coord = ((Point)feature.getDefaultGeometry()).getCoordinate();
			sb.append(DxfGroup.toString(10, coord.x, precision));
			sb.append(DxfGroup.toString(20, coord.y, precision));
			
			// Robuster machen, falls ori, hali etc fehlen...
			Double ori = (Double) feature.getAttribute("ori");
			if (ori != null) {
				sb.append(DxfGroup.toString(50, (Double) feature.getAttribute("ori")));
			} else {
				sb.append(DxfGroup.toString(50, 0.0));
			}
			
			Integer hali = (Integer) feature.getAttribute("hali");
			if (hali != null) {
				sb.append(DxfGroup.toString(72, (Integer) feature.getAttribute("hali")));
			} else {
				sb.append(DxfGroup.toString(72, 1));
			}

			// DXF kennt im Gegensatz zu Interlis nur 3 vali-Werte
			Integer dxfVali = 2;
			//Integer vali = ((Integer) feature.getAttribute("VALI") > 3 ? 3 : (Integer) feature.getAttribute("VALI"));
			if ((Integer) feature.getAttribute("vali") != null) {
				int vali = (Integer) feature.getAttribute("vali");
				switch(vali) {
				case 0:
					dxfVali = 3; break;
				case 1: 
					dxfVali = 3; break;
				case 2:
					dxfVali = 2; break;
				case 3:
					dxfVali = 1; break;
				case 4:
					dxfVali = 0; break;
				default:
					dxfVali = 2; 
				}
			} 

			sb.append(DxfGroup.toString(73, dxfVali.toString()));
			sb.append(DxfGroup.toString(11, coord.x, precision));
			sb.append(DxfGroup.toString(21, coord.y, precision));       

		}
		else {
			logger.warn("Should not reach here.");
		}
		return sb.toString();
	}

	public static String lineString2Dxf(SimpleFeature feature, String layerName) throws Exception {
		LineString geom = (LineString)feature.getDefaultGeometry();
		Coordinate[] coords = geom.getCoordinates();
		StringBuffer sb = new StringBuffer(DxfGroup.toString(0, "POLYLINE"));

		sb.append(DxfGroup.toString(8, layerName));
		
		sb.append(DxfGroup.toString(66, 1));
		sb.append(DxfGroup.toString(10, "0.0"));
		sb.append(DxfGroup.toString(20, "0.0"));
		if (!Double.isNaN(coords[0].z)) sb.append(DxfGroup.toString(30, "0.0"));
		sb.append(DxfGroup.toString(70, 32));

		for (int i = 0 ; i < coords.length ; i++) {
			sb.append(DxfGroup.toString(0, "VERTEX"));
			sb.append(DxfGroup.toString(8, layerName));
			sb.append(DxfGroup.toString(10, coords[i].x, precision));
			sb.append(DxfGroup.toString(20, coords[i].y, precision));
			if (!Double.isNaN(coords[i].z)) sb.append(DxfGroup.toString(30, coords[i].z, precision));
			sb.append(DxfGroup.toString(70, 32));
		}
		sb.append(DxfGroup.toString(0, "SEQEND"));
		return sb.toString();
	}

	public static String polygon2Dxf(SimpleFeature feature, String layerName, boolean suffix) {
		Polygon geom = (Polygon)feature.getDefaultGeometry();
		Coordinate[] coords = geom.getExteriorRing().getCoordinates();
		StringBuffer sb = new StringBuffer(DxfGroup.toString(0, "POLYLINE"));
		sb.append(DxfGroup.toString(8, layerName));
		sb.append(DxfGroup.toString(66, 1));
		sb.append(DxfGroup.toString(10, "0.0"));
		sb.append(DxfGroup.toString(20, "0.0"));
		if (!Double.isNaN(coords[0].z)) sb.append(DxfGroup.toString(30, "0.0"));
		// Ziegler 22-07-2008
		//sb.append(DxfGroup.toString(70, 9));
		sb.append(DxfGroup.toString(70, 32));
		for (int i = 0 ; i < coords.length ; i++) {
			sb.append(DxfGroup.toString(0, "VERTEX"));
			sb.append(DxfGroup.toString(8, layerName));
			sb.append(DxfGroup.toString(10, coords[i].x, precision));
			sb.append(DxfGroup.toString(20, coords[i].y, precision));
			if (!Double.isNaN(coords[i].z)) sb.append(DxfGroup.toString(30, coords[i].z, precision));
			//sb.append(DxfGroup.toString(70, 32));
			// Ziegler 22-07-2008
			sb.append(DxfGroup.toString(70, 32));
		}
		sb.append(DxfGroup.toString(0, "SEQEND"));
		for (int h = 0 ; h < geom.getNumInteriorRing() ; h++) {
			//System.out.println("polygon2Dxf (hole)" + suffix);
			sb.append(DxfGroup.toString(0, "POLYLINE"));
			if (suffix) sb.append(DxfGroup.toString(8, layerName+"_"));
			else sb.append(DxfGroup.toString(8, layerName));
			sb.append(DxfGroup.toString(66, 1));
			sb.append(DxfGroup.toString(10, "0.0"));
			sb.append(DxfGroup.toString(20, "0.0"));
			if (!Double.isNaN(coords[0].z)) sb.append(DxfGroup.toString(30, "0.0"));
			// Ziegler 22-07-2008
			//sb.append(DxfGroup.toString(70, 9));
			sb.append(DxfGroup.toString(70, 32));
			coords = geom.getInteriorRingN(h).getCoordinates();
			for (int i = 0 ; i < coords.length ; i++) {
				sb.append(DxfGroup.toString(0, "VERTEX"));
				if (suffix) sb.append(DxfGroup.toString(8, layerName+"_"));
				else sb.append(DxfGroup.toString(8, layerName));
				sb.append(DxfGroup.toString(10, coords[i].x, precision));
				sb.append(DxfGroup.toString(20, coords[i].y, precision));
				if (!Double.isNaN(coords[i].z)) sb.append(DxfGroup.toString(30, coords[i].z, precision));
				//sb.append(DxfGroup.toString(70, 32));
				// Ziegler 22-07-2008
				sb.append(DxfGroup.toString(70, 32));
			}
			sb.append(DxfGroup.toString(0, "SEQEND"));
		}
//		logger.debug(sb.toString());
		return sb.toString();
	}

}

