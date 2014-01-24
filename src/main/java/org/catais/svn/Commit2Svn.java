package org.catais.svn;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.catais.utils.DeleteFiles;

import org.tmatesoft.svn.core.*;
import org.tmatesoft.svn.core.internal.io.dav.DAVRepositoryFactory;
import org.tmatesoft.svn.core.internal.io.svn.SVNRepositoryFactoryImpl;
import org.tmatesoft.svn.core.internal.wc.DefaultSVNOptions;
import org.tmatesoft.svn.core.wc.ISVNOptions;
import org.tmatesoft.svn.core.wc.SVNClientManager;
import org.tmatesoft.svn.core.wc.SVNCommitClient;
import org.tmatesoft.svn.core.wc.SVNDiffClient;
import org.tmatesoft.svn.core.wc.SVNRevision;
import org.tmatesoft.svn.core.wc.SVNUpdateClient;
import org.tmatesoft.svn.core.wc.SVNWCUtil;

public class Commit2Svn {
	private static Logger logger = Logger.getLogger(Commit2Svn.class);
	
	private HashMap params = null;
	private String importSourceDir = null;
	private String importDestinationDir = null;
	private String svnurl = null;
	private String svnuser = null;
	private String svnpwd = null;


	public Commit2Svn(HashMap params) {
		logger.setLevel(Level.INFO);
		
		this.params = params;
		readParams();
	}

	public void run() throws Exception {
    	
		DAVRepositoryFactory.setup();
		SVNURL repositoryURL = null;
		try {
			repositoryURL = SVNURL.parseURIEncoded(svnurl);
			logger.debug(repositoryURL.toString());
			DefaultSVNOptions options = SVNWCUtil.createDefaultOptions(true);
			SVNClientManager ourClientManager = SVNClientManager.newInstance(options, this.svnuser, this.svnpwd); 
			// Falls commit stattgefunden hat oder mit login/pwd ausgechecked wurde,
			// ist eine Authentifizierung unnötig. Sonst kann als zusätzliche Parameter
			// Login und Passwort angegeben werden. AuthenticationManager unnötig?
			SVNCommitClient commitClient = ourClientManager.getCommitClient();

			File destFilePath = new File(importDestinationDir);
			logger.info("Commit: " + destFilePath.toString());

			SVNCommitInfo commitInfo = commitClient.doCommit(new File[]{destFilePath}, true, "dm01avso24", true, true);
			if (commitInfo.getErrorMessage() == null) {
				logger.info("Directory commited: " + importDestinationDir);
				logger.info("Revision: " + commitInfo.getNewRevision());
			} else {
				logger.error(commitInfo.getErrorMessage());

			}
		} catch (SVNException e) {
			logger.error("SVNException");
			e.printStackTrace();
			logger.error(e.getMessage());
		}

	}
	
	private void readParams() {
		importSourceDir = (String) params.get("importSourceDir");
		logger.debug("Import Source Directory: " + importSourceDir);
		if (importSourceDir == null) {
			throw new IllegalArgumentException("Import source dir not set.");
		}	
		
		importDestinationDir = (String) params.get("importDestinationDir");
		logger.debug("Import destination Directory: " + importDestinationDir);	
		if (importDestinationDir == null) {
			throw new IllegalArgumentException("Import destination dir not set.");
		}	
		
		svnurl = (String) params.get("svnurl");
		logger.debug("svnurl: " + svnurl);	
		if (svnurl == null) {
			throw new IllegalArgumentException("svnurl not set.");
		}
		
		svnuser = (String) params.get("svnuser");
		logger.debug("svnuser: " + svnuser);	
		if (svnuser == null) {
			throw new IllegalArgumentException("svnuser not set.");
		}
		
		svnpwd = (String) params.get("svnpwd");
		logger.debug("svnpwd: " + svnpwd);	
		if (svnpwd == null) {
			throw new IllegalArgumentException("svnpwd not set.");
		}
	}
}
