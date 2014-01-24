package org.catais.fusion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.catais.importdata.ImportData;
import org.catais.utils.IOUtils;

public class Fusion {
	private static Logger logger = Logger.getLogger(Fusion.class);
	
	String type = null;
	String prefix = "";
	HashMap params = null;
	String importSourceDir = null;
	String destinationDir = null;

	public Fusion(HashMap params, String type) {
		logger.setLevel(Level.INFO);
		
		this.type = type;
		this.params = params;
		readParams();
	}

	
	private void readParams() {
		importSourceDir = (String) params.get("importSourceDir");
		logger.debug("Import Source Directory: " + importSourceDir);
		if (importSourceDir == null) {
			throw new IllegalArgumentException("Import source dir not set.");
		}	
		
		if (type.equalsIgnoreCase("so")) {
			destinationDir = (String) params.get("importDestinationDir");
			logger.debug("Import destination Directory: " + destinationDir);		
			if (destinationDir == null) {
				throw new IllegalArgumentException("Destination dir not set.");
			}
		} else if (type.equalsIgnoreCase("ch")) { 
			destinationDir = (String) params.get("ili2chDir");
			logger.debug("ili2chDir:: " + destinationDir);		
			if (destinationDir == null) {
				throw new IllegalArgumentException("ili2chDir dir not set.");
			}
			prefix = "ch_";
		}
	}

	
	public void run() throws IOException {
		
		HashMap gem_map = new HashMap();
		
    	File tempDir = IOUtils.createTempDirectory("itf2avdpoolng");
		InputStream is =  Fusion.class.getResourceAsStream("gemeindefusionen.txt");
		File txtFile = new File(tempDir, "gemeindefusionen.txt");
		IOUtils.copy(is, txtFile);

		BufferedReader br = new BufferedReader(new FileReader(txtFile));
		String line;
		while ((line = br.readLine()) != null) {
		   String gem_bfs = line.split(":")[0];
		   String[] operate = line.split(":")[1].toString().split(",");

		   logger.debug("Gemeinde: " + gem_bfs);

		   logger.debug(this.destinationDir);
		   
		   // Achtung: wir überschreiben Los 0. Am besten würden wir für nicht
		   // fusionierte Gemeinden nur Los >= 1 verwenden. Mit dem Nachteil, dass
		   // nach der Fusion mehr aus der DB gelöscht werden muss. Aber: so kann man immer 
		   // noch auf die einzelnen Lose zugreifen. Sonst gibts alle Lose einzeln ausser das
		   // Los 0 nicht (weil ja da alles drin ist).
		   String outZipFileName = destinationDir.trim() + File.separator + prefix  + gem_bfs + "00.zip";
		   FileOutputStream zipfile = new FileOutputStream(outZipFileName);
		   ZipOutputStream zipOutputStream = new ZipOutputStream(zipfile);

		   for (int i = 0; i < operate.length; i++) {
			   String fileName = destinationDir.trim() + File.separator + prefix + operate[i] + ".itf";

			   File itfFile = new File(fileName);
			   if (itfFile.exists()) {
				   ZipEntry iliZipEntry = new ZipEntry(itfFile.getName());
				   zipOutputStream.putNextEntry(iliZipEntry);
				   new FileInputStream(itfFile).getChannel().transferTo(0, itfFile.length(), Channels.newChannel(zipOutputStream));				    
			   } else {
				   logger.error("File not found for fusion: " + fileName);
			   }
			  
		   }
		   
		   // Add the meta files.
		   InputStream isIli =  Fusion.class.getResourceAsStream("dm01avso24.ili");
		   File iliFile = new File(tempDir, "dm01avso24.ili");
		   IOUtils.copy(isIli, iliFile);

		   ZipEntry iliZipEntry = new ZipEntry(iliFile.getName());
		   zipOutputStream.putNextEntry(iliZipEntry);
		   new FileInputStream(iliFile).getChannel().transferTo(0, iliFile.length(), Channels.newChannel(zipOutputStream));
		   
		   // more meta to come....
		   
		   zipOutputStream.closeEntry();
		   zipOutputStream.close();

		   
		}
		br.close();
		
	}

}
