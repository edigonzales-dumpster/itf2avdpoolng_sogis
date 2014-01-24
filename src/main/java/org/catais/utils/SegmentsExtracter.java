/*
 * The Unified Mapping Platform (JUMP) is an extensible, interactive GUI
 * for visualizing and manipulating spatial features with geometry and attributes.
 *
 * Copyright (C) 2003 Vivid Solutions
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 *
 * For more information, contact:
 *
 * Vivid Solutions
 * Suite #1A
 * 2328 Government Street
 * Victoria BC  V8T 5G5
 * Canada
 *
 * (250)385-6040
 * www.vividsolutions.com
 */

/*
 * Modified: Stefan Ziegler / 2010-02-13
 * 
 */

package org.catais.utils;

import java.util.*;

import ch.interlis.ili2c.metamodel.SurfaceOrAreaType;
import ch.interlis.iom.IomObject;
import ch.interlis.iox_j.jts.Jts2iox;

import com.vividsolutions.jts.geom.*;

import org.apache.log4j.Logger;
import org.catais.exportdata.ExportData;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;

public class SegmentsExtracter {
	private static Logger logger = Logger.getLogger(SegmentsExtracter.class);

	private boolean countZeroLengthSegments = false;

	public SegmentsExtracter() {
	}

	public Collection getSegments(FeatureCollection fc) {
		Map segmentMap = new TreeMap();

		add(fc, segmentMap);

		return segmentMap.keySet();

	}

	private void add(FeatureCollection fc, Map segmentMap)  
	{


		int totalFeatures = fc.size();
		int j = 0;
		for (Iterator i = fc.iterator(); i.hasNext(); ) {
			SimpleFeature feature = (SimpleFeature) i.next();
			j++;
			add(feature, segmentMap);
		}
	}

	private void add(SimpleFeature f, Map segmentMap)
	{
		Geometry g = (Geometry) f.getDefaultGeometry();

		int numGeoms = g.getNumGeometries();
		for(int j=0; j<numGeoms; j++) {

			Polygon p = (Polygon) g.getGeometryN(j);

			// Shell of the Polygon
			{
				LineString exteriorRing = p.getExteriorRing();
				Coordinate[] coord = exteriorRing.getCoordinates();
				for (int m = 0; m < coord.length - 1; m++) {
					add(coord[m], coord[m + 1], segmentMap);					
				}
			}

			// Holes of the Polygon
			int numHoles = p.getNumInteriorRing();
			for(int k=0; k<numHoles; k++) {
				LineString hole = (LineString) p.getInteriorRingN(k);

				Coordinate[] coord = hole.getCoordinates();
				for (int m = 0; m < coord.length - 1; m++) {
					add(coord[m], coord[m + 1], segmentMap);
				}
			}
		}
	}

	private void add(Coordinate p0, Coordinate p1, Map segmentMap)
	{
		// check for zero-length segment
		boolean isZeroLength = p0.equals(p1);
		if (! countZeroLengthSegments && isZeroLength)
			return;

		LineSegment lineseg = new LineSegment(p0, p1);
		lineseg.normalize();

		SegmentCount count = (SegmentCount) segmentMap.get(lineseg);
		if (count == null) {
			segmentMap.put(lineseg, new SegmentCount(1));
		}
		else {
			count.increment();
		}
		//segmentSet.add(lineseg);
	}



	private class SegmentCount {
		private int count = 0;
		public SegmentCount(int value) {
			this.count = value;
		}
		public int getCount() { return count; }
		public void increment() {count++;}
	}
}