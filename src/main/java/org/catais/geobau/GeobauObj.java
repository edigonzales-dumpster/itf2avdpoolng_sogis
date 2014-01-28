package org.catais.geobau;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.catais.importdata.ImportData;
import org.catais.utils.IOUtils;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Envelope;

public class GeobauObj {
	private static Logger logger = Logger.getLogger(GeobauObj.class);
	
	BufferedWriter fw = null;
	String geobauDir = null;
	String fosnr = null;
	String outputFileName = null;
	
	public GeobauObj(String geobauDir, String itfFileName) throws NumberFormatException, FileNotFoundException, UnsupportedEncodingException {
		logger.setLevel(Level.INFO);
		
		this.geobauDir = geobauDir;
		fosnr = Integer.valueOf(itfFileName.substring(0, 4)).toString();
		outputFileName = geobauDir + File.separator +  fosnr + ".dxf";
		FileOutputStream dxffile = new FileOutputStream(outputFileName);	
		logger.info(geobauDir + File.separator +  fosnr + ".dxf");
		fw = new BufferedWriter(new OutputStreamWriter(dxffile, "ISO-8859-1"));	
	}
	
	public void zip() throws FileNotFoundException, IOException {
		logger.info("Zipping file...");
    	File destinationFile = new File(outputFileName);    		        	
		String outputZipFileName = geobauDir + File.separator +  fosnr + ".zip";
		
		// We add also some metadata files to the zipfile.

		File tempDir = IOUtils.createTempDirectory("itf2avdpoolng");
		InputStream isLayerCode =  GeobauObj.class.getResourceAsStream("DXF_Geobau_Layerdefinition.pdf");
		File layerCodeFile = new File(tempDir, "DXF_Geobau_Layerdefinition.pdf");
		IOUtils.copy(isLayerCode, layerCodeFile);
		
		InputStream isHinweise =  GeobauObj.class.getResourceAsStream("Hinweise.pdf");
		File hinweiseFile = new File(tempDir, "Hinweise.pdf");
		IOUtils.copy(isHinweise, hinweiseFile);

		FileOutputStream zipfile = new FileOutputStream(outputZipFileName);
		ZipOutputStream zipOutputStream = new ZipOutputStream(zipfile);

		// Layer code
		ZipEntry layerCodeZipEntry = new ZipEntry(layerCodeFile.getName());
		zipOutputStream.putNextEntry(layerCodeZipEntry);
		new FileInputStream(layerCodeFile).getChannel().transferTo(0, layerCodeFile.length(), Channels.newChannel(zipOutputStream));

		// Hinweise
		ZipEntry hinweiseZipEntry = new ZipEntry(hinweiseFile.getName());
		zipOutputStream.putNextEntry(hinweiseZipEntry);
		new FileInputStream(hinweiseFile).getChannel().transferTo(0, hinweiseFile.length(), Channels.newChannel(zipOutputStream));

		// DXF
		ZipEntry zipEntry = new ZipEntry(destinationFile.getName());
		zipOutputStream.putNextEntry(zipEntry);
		new FileInputStream(destinationFile).getChannel().transferTo(0, destinationFile.length(), Channels.newChannel(zipOutputStream));
		
		zipOutputStream.closeEntry();
		zipOutputStream.close();
		logger.info("File zipped: " + outputZipFileName);
	}
	
	
	public void write(FeatureCollection fc, String layerName, int precision, boolean header, boolean footer, String sqlquery) throws IOException, Exception {
		if (header) {
			Envelope envelope = envelope = fc.getBounds();
	        fw.write(DxfGroup.toString(0, "SECTION"));
	        fw.write(DxfGroup.toString(2, "HEADER"));
	        fw.write(DxfGroup.toString(9, "$EXTMAX"));
	        fw.write(DxfGroup.toString(10, envelope.getMaxX(), 6));
	        fw.write(DxfGroup.toString(20, envelope.getMaxY(), 6));
	        fw.write(DxfGroup.toString(9, "$EXTMIN"));
	        fw.write(DxfGroup.toString(10, envelope.getMinX(), 6));
	        fw.write(DxfGroup.toString(20, envelope.getMinY(), 6));
	        fw.write(DxfGroup.toString(9, "$LIMMAX"));
	        fw.write(DxfGroup.toString(10, envelope.getMaxX(), 6));
	        fw.write(DxfGroup.toString(20, envelope.getMaxY(), 6));
	        fw.write(DxfGroup.toString(9, "$LIMMIN"));
	        fw.write(DxfGroup.toString(10, envelope.getMinX(), 6));
	        fw.write(DxfGroup.toString(20, envelope.getMinY(), 6));
	        fw.write(DxfGroup.toString(0, "ENDSEC"));
	    
	        // BLOCK (Symbole)             
	        fw.write(DxfGroup.toString(0, "SECTION"));
	        fw.write(DxfGroup.toString(2, "BLOCKS"));
	        
	        // GP Bolzen                
	        fw.write(DxfGroup.toString(0, "BLOCK"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(70, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(2, "GPBOL"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "0.5"));
	        fw.write(DxfGroup.toString(0, "ENDBLK"));
	        fw.write(DxfGroup.toString(8, "0")); 
	        
	        // GP Rohr                
	        fw.write(DxfGroup.toString(0, "BLOCK"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(70, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(2, "GPROH"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "0.5"));
	        fw.write(DxfGroup.toString(0, "ENDBLK"));
	        fw.write(DxfGroup.toString(8, "0"));   

	        // GP Pfahl                
	        fw.write(DxfGroup.toString(0, "BLOCK"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(70, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(2, "GPPFA"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "0.5"));
	        fw.write(DxfGroup.toString(0, "ENDBLK"));
	        fw.write(DxfGroup.toString(8, "0"));     
	        
	        // GP unversichert
	        fw.write(DxfGroup.toString(0, "BLOCK"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(70, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(2, "GPUV"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "0.1"));
	        fw.write(DxfGroup.toString(0, "ENDBLK"));
	        fw.write(DxfGroup.toString(8, "0"));

	        // GP Markstein
	        fw.write(DxfGroup.toString(0, "BLOCK"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(70, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(2, "GPSTE"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "0.7"));
	        fw.write(DxfGroup.toString(0, "ENDBLK"));
	        fw.write(DxfGroup.toString(8, "0"));   
	        
	        // GP Kunststoff
	        fw.write(DxfGroup.toString(0, "BLOCK"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(70, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(2, "GPKST"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "0.7"));
	        fw.write(DxfGroup.toString(0, "ENDBLK"));
	        fw.write(DxfGroup.toString(8, "0"));   

            // GP Kreuz
            fw.write(DxfGroup.toString(0, "BLOCK"));
            fw.write(DxfGroup.toString(8, "0"));
            fw.write(DxfGroup.toString(70, "0"));
            fw.write(DxfGroup.toString(10, "0.0"));
            fw.write(DxfGroup.toString(20, "0.0"));
            fw.write(DxfGroup.toString(30, "0.0"));
            fw.write(DxfGroup.toString(2, "GPKRZ"));
            fw.write(DxfGroup.toString(0, "CIRCLE"));
            fw.write(DxfGroup.toString(8, "0"));
            fw.write(DxfGroup.toString(10, "0.0"));
            fw.write(DxfGroup.toString(20, "0.0"));
            fw.write(DxfGroup.toString(30, "0.0"));
            fw.write(DxfGroup.toString(40, "0.5"));
            fw.write(DxfGroup.toString(0, "LINE"));
            fw.write(DxfGroup.toString(8, "0"));
            fw.write(DxfGroup.toString(10, "-0.849"));
            fw.write(DxfGroup.toString(20, "-0.849"));
            fw.write(DxfGroup.toString(30, "0.000"));
            fw.write(DxfGroup.toString(11, "-0.283"));
            fw.write(DxfGroup.toString(21, "-0.283"));
            fw.write(DxfGroup.toString(31, "0.000"));
            fw.write(DxfGroup.toString(0, "LINE"));
            fw.write(DxfGroup.toString(8, "0"));
            fw.write(DxfGroup.toString(10, "-0.284"));
            fw.write(DxfGroup.toString(20, "0.284"));
            fw.write(DxfGroup.toString(30, "0.000"));
            fw.write(DxfGroup.toString(11, "-0.849"));
            fw.write(DxfGroup.toString(21, "0.849"));
            fw.write(DxfGroup.toString(31, "0.000"));
            fw.write(DxfGroup.toString(0, "LINE"));
            fw.write(DxfGroup.toString(8, "0"));
            fw.write(DxfGroup.toString(10, "0.283"));
            fw.write(DxfGroup.toString(20, "0.283"));
            fw.write(DxfGroup.toString(30, "0.000"));
            fw.write(DxfGroup.toString(11, "0.849"));
            fw.write(DxfGroup.toString(21, "0.849"));
            fw.write(DxfGroup.toString(31, "0.000"));
            fw.write(DxfGroup.toString(0, "LINE"));
            fw.write(DxfGroup.toString(8, "0"));
            fw.write(DxfGroup.toString(10, "0.849"));
            fw.write(DxfGroup.toString(20, "-0.849"));
            fw.write(DxfGroup.toString(30, "0.000"));
            fw.write(DxfGroup.toString(11, "0.283"));
            fw.write(DxfGroup.toString(21, "-0.283"));
            fw.write(DxfGroup.toString(31, "0.000"));
            fw.write(DxfGroup.toString(0, "ENDBLK"));
            fw.write(DxfGroup.toString(8, "0"));   
            
	        // HGP
	        fw.write(DxfGroup.toString(0, "BLOCK"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(70, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(2, "HGP"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "1.5"));
	        fw.write(DxfGroup.toString(0, "ENDBLK"));
	        fw.write(DxfGroup.toString(8, "0"));   

            
	        // LFP1 
	        fw.write(DxfGroup.toString(0, "BLOCK"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(70, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(2, "LFP1"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "0.8"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "1.3"));
	        fw.write(DxfGroup.toString(0, "ENDBLK")); 
	        fw.write(DxfGroup.toString(8, "0"));    
	        
	        // LFP2 
	        fw.write(DxfGroup.toString(0, "BLOCK"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(70, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(2, "LFP2"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "0.8"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "1.3"));
	        fw.write(DxfGroup.toString(0, "ENDBLK")); 
	        fw.write(DxfGroup.toString(8, "0"));  

	        // HFP1
	        fw.write(DxfGroup.toString(0, "BLOCK"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(70, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(2, "HFP1"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "1.0"));
	        fw.write(DxfGroup.toString(0, "ENDBLK")); 
	        fw.write(DxfGroup.toString(8, "0")); 
	        
	        // HFP2
	        fw.write(DxfGroup.toString(0, "BLOCK"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(70, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(2, "HFP2"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "1.0"));
	        fw.write(DxfGroup.toString(0, "ENDBLK")); 
	        fw.write(DxfGroup.toString(8, "0")); 
	        
	        // HFP3
	        fw.write(DxfGroup.toString(0, "BLOCK"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(70, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(2, "HFP3"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "1.0"));
	        fw.write(DxfGroup.toString(0, "ENDBLK")); 
	        fw.write(DxfGroup.toString(8, "0")); 
	        
	        // LFP3 Stein
	        fw.write(DxfGroup.toString(0, "BLOCK"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(70, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(2, "LFP3ST"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "0.8"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "1.3"));
	        fw.write(DxfGroup.toString(0, "ENDBLK")); 
	        fw.write(DxfGroup.toString(8, "0")); 
	        
	        // LFP3 Bolzen
	        fw.write(DxfGroup.toString(0, "BLOCK"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(70, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(2, "LFP3BO"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "1.0"));
	        fw.write(DxfGroup.toString(0, "ENDBLK")); 
	        fw.write(DxfGroup.toString(8, "0")); 
	        
	        // LFP3 uv
	        fw.write(DxfGroup.toString(0, "BLOCK"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(70, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(2, "LFP3UV"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "0.3"));
	        fw.write(DxfGroup.toString(0, "ENDBLK")); 
	        fw.write(DxfGroup.toString(8, "0")); 
	        
	        // LFP3 Kreuz
	        fw.write(DxfGroup.toString(0, "BLOCK"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(70, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(2, "LFP3KR"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "0.4"));
	        fw.write(DxfGroup.toString(0, "LINE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "-0.849"));
	        fw.write(DxfGroup.toString(20, "-0.849"));
	        fw.write(DxfGroup.toString(30, "0.000"));
	        fw.write(DxfGroup.toString(11, "-0.283"));
	        fw.write(DxfGroup.toString(21, "-0.283"));
	        fw.write(DxfGroup.toString(31, "0.000"));
	        fw.write(DxfGroup.toString(0, "LINE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "-0.284"));
	        fw.write(DxfGroup.toString(20, "0.284"));
	        fw.write(DxfGroup.toString(30, "0.000"));
	        fw.write(DxfGroup.toString(11, "-0.849"));
	        fw.write(DxfGroup.toString(21, "0.849"));
	        fw.write(DxfGroup.toString(31, "0.000"));
	        fw.write(DxfGroup.toString(0, "LINE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.283"));
	        fw.write(DxfGroup.toString(20, "0.283"));
	        fw.write(DxfGroup.toString(30, "0.000"));
	        fw.write(DxfGroup.toString(11, "0.849"));
	        fw.write(DxfGroup.toString(21, "0.849"));
	        fw.write(DxfGroup.toString(31, "0.000"));
	        fw.write(DxfGroup.toString(0, "LINE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.849"));
	        fw.write(DxfGroup.toString(20, "-0.849"));
	        fw.write(DxfGroup.toString(30, "0.000"));
	        fw.write(DxfGroup.toString(11, "0.283"));
	        fw.write(DxfGroup.toString(21, "-0.283"));
	        fw.write(DxfGroup.toString(31, "0.000"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "1.200667"));
	        fw.write(DxfGroup.toString(0, "ENDBLK"));
	        fw.write(DxfGroup.toString(8, "0")); 
	        
	        // EO Punkte
	        fw.write(DxfGroup.toString(0, "BLOCK"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(70, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(2, "EOPNT"));
	        fw.write(DxfGroup.toString(0, "CIRCLE"));
	        fw.write(DxfGroup.toString(8, "0"));
	        fw.write(DxfGroup.toString(10, "0.0"));
	        fw.write(DxfGroup.toString(20, "0.0"));
	        fw.write(DxfGroup.toString(30, "0.0"));
	        fw.write(DxfGroup.toString(40, "0.4"));
	        fw.write(DxfGroup.toString(0, "ENDBLK")); 
	        fw.write(DxfGroup.toString(8, "0")); 
	
	        fw.write(DxfGroup.toString(0, "ENDSEC"));
	        
            fw.write(DxfGroup.toString(0, "SECTION"));
            fw.write(DxfGroup.toString(2, "ENTITIES"));

		}
		logger.debug("Size: " + fc.size());
		SimpleFeatureIterator iterator = (SimpleFeatureIterator) fc.features();
		
		try {
			while (iterator.hasNext()) {
				SimpleFeature feature = (SimpleFeature) iterator.next();
//				logger.debug(feature.getDefaultGeometry().toString());
				fw.write(DxfENTITY.feature2Dxf(feature, layerName, false));
			}
		} catch (Exception e) {
			logger.error(layerName);
			logger.error(sqlquery);
			logger.error(fc.size());
			e.printStackTrace();
			logger.error(e.getMessage());
		} finally {
			iterator.close();
		}
		
        if(footer) {
        	fw.write(DxfGroup.toString(0, "ENDSEC"));
            fw.write(DxfGroup.toString(0, "EOF"));
            fw.flush();
        }
	}
	

}
