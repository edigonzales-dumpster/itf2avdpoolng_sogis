package org.catais.exportdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import ch.interlis.ili2c.Ili2cException;
import ch.interlis.iox.IoxException;

public class ExportData {
	private static Logger logger = Logger.getLogger(ExportData.class);
	
	HashMap params = null;
	String exportModelName = null;
	String exportDestinationDir = null;
	
	
	public ExportData(HashMap params) {
		logger.setLevel(Level.DEBUG);
		
		this.params = params;
	}

	
	public void run() throws IllegalArgumentException, Ili2cException, IoxException, IOException, Exception {
		
			IliWriter writer = new IliWriter(params);
			writer.run();
		
	}

	
}
