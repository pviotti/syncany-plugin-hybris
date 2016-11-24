package org.syncany.plugins.hybris;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.syncany.config.Config;
import org.syncany.plugins.hybris.HybrisTransferManager.HybrisReadAfterWriteConsistentFeatureExtension;
import org.syncany.plugins.transfer.AbstractTransferManager;
import org.syncany.plugins.transfer.StorageException;
import org.syncany.plugins.transfer.StorageMoveException;
import org.syncany.plugins.transfer.features.ReadAfterWriteConsistent;
import org.syncany.plugins.transfer.features.ReadAfterWriteConsistentFeatureExtension;
import org.syncany.plugins.transfer.files.ActionRemoteFile;
import org.syncany.plugins.transfer.files.CleanupRemoteFile;
import org.syncany.plugins.transfer.files.DatabaseRemoteFile;
import org.syncany.plugins.transfer.files.MultichunkRemoteFile;
import org.syncany.plugins.transfer.files.RemoteFile;
import org.syncany.plugins.transfer.files.SyncanyRemoteFile;
import org.syncany.plugins.transfer.files.TempRemoteFile;
import org.syncany.plugins.transfer.files.TransactionRemoteFile;

import fr.eurecom.hybris.Hybris;
import fr.eurecom.hybris.HybrisException;

/**
 * Hybris SyncAny Transfer Manager
 * @author PV
 */
@ReadAfterWriteConsistent(extension = HybrisReadAfterWriteConsistentFeatureExtension.class)
public class HybrisTransferManager extends AbstractTransferManager {

	private static final Logger logger = Logger.getLogger(HybrisTransferManager.class.getSimpleName());

	private Hybris hybris;
	
	private String multichunksPath;
	private String databasesPath;
	private String actionsPath;
	private String transactionsPath;
	private String tempPath;

	public HybrisTransferManager(HybrisTransferSettings connection, Config config) {
		super(connection, config);

		this.multichunksPath = "mc-";
		this.databasesPath = "db-";
		this.actionsPath = "ac-";
		this.transactionsPath = "tx-";
		this.tempPath = "tmp-";
	}

	public HybrisTransferSettings getSettings() {
		return (HybrisTransferSettings) settings;
	}

	public void connect() throws StorageException {
		
		if (hybris == null) {
			try {
	            hybris = new Hybris(getSettings().getPropertyFile());
	            logger.log(Level.INFO, "Hybris initialized.");
	        } catch (HybrisException e) {
	        	throw new StorageException("Invalid service found", e);
	        }
		}
	}

	public void disconnect() throws StorageException {
		// Nothing
		logger.log(Level.INFO, "Hybris disconnect (not implemented)");
		// hybris.shutdown();
	}

	public void init(boolean createIfRequired) throws StorageException {
		connect();
	}

	public void download(RemoteFile remoteFile, File localFile) throws StorageException {
		connect();

		File tempFile = null;
		String remotePath = getRemoteFile(remoteFile);
		try {
			// Download
			byte[] data;
			if (isDataRemoteFile(remoteFile))
				data = hybris.get(remotePath);
			else
				data = hybris.rmds.rawRead(remotePath);
			if (data == null) throw new Exception();
			logger.log(Level.FINE, "- Downloaded: " + remotePath + " ...");
			
			tempFile = createTempFile(remoteFile.getName());
			FileUtils.writeByteArrayToFile(tempFile, data);
			// Move to final location
			if (localFile.exists())
				localFile.delete();
			FileUtils.moveFile(tempFile, localFile);
		} catch (Exception e) {
			if (tempFile != null)
				tempFile.delete();
			throw new StorageException("Unable to download file '" + remoteFile.getName(), e);
		}
	}

	public void upload(File localFile, RemoteFile remoteFile) throws StorageException {
		connect();

		String remotePath = getRemoteFile(remoteFile);
		try {
			byte[] data = FileUtils.readFileToByteArray(localFile);
			if (isDataRemoteFile(remoteFile))
				hybris.put(remotePath, data);
			else
				hybris.rmds.rawWrite(remotePath, data);
			logger.log(Level.FINE, "- Uploaded: " + remotePath + " ...");
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Cannot upload " + localFile + " to " + remotePath, e);
			throw new StorageException(e);
		}
	}

	public boolean delete(RemoteFile remoteFile) throws StorageException {
		connect();

		String remotePath = getRemoteFile(remoteFile);
		try {
			if (isDataRemoteFile(remoteFile))
				hybris.delete(remotePath);
			else 
				hybris.rmds.rawDelete(remotePath);
			logger.log(Level.FINE, "- Removed: " + remotePath + " ...");
			return true;
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Unable to delete remote file " + remotePath, e);
			throw new StorageException(e);
		}
	}

	public void move(RemoteFile sourceFile, RemoteFile targetFile) throws StorageException {
		connect();

		String sourceRemotePath = getRemoteFile(sourceFile);
		String targetRemotePath = getRemoteFile(targetFile);
		byte[] data;
		try {
			if (isDataRemoteFile(sourceFile)) {
				data = hybris.get(sourceRemotePath);
				hybris.put(targetRemotePath, data);
				hybris.delete(sourceRemotePath);
			} else {
				data = hybris.rmds.rawRead(sourceRemotePath);
				hybris.rmds.rawWrite(targetRemotePath, data);
				hybris.rmds.rawDelete(sourceRemotePath);
			}
			
			logger.log(Level.FINE, "- Moved: " + sourceRemotePath + " -> " + targetRemotePath);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Cannot move " + sourceRemotePath + " to " + targetRemotePath, e);
			throw new StorageMoveException(e);
		}
	}

	public <T extends RemoteFile> Map<String, T> list(Class<T> remoteFileClass) throws StorageException {
		connect();

		try {
			// List
			List<String> objects;
			if (remoteFileClass.equals(MultichunkRemoteFile.class))
				objects = hybris.list();
			else {
				objects = hybris.rmds.rawList();
				Iterator<String> i = objects.iterator();
				while (i.hasNext()) {
				   String s = i.next();
				   if (!s.startsWith(getRemoteFilePath(remoteFileClass)))
					   i.remove();
				}
			}

			// Create RemoteFile objects
			Map<String, T> remoteFiles = new HashMap<String, T>();
			for (String object : objects) {
				String simpleRemoteName = object.substring(object.indexOf("-") + 1);

				if (simpleRemoteName.length() > 0) {
					try {
						T remoteFile = RemoteFile.createRemoteFile(simpleRemoteName, remoteFileClass);
						remoteFiles.put(simpleRemoteName, remoteFile);
					} catch (Exception e) {
						logger.log(Level.INFO, "Cannot create instance of " + remoteFileClass.getSimpleName() + " for object " + simpleRemoteName
										+ "; maybe invalid file name pattern. Ignoring file.");
					}
				}
			}

			return remoteFiles;
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Unable to list.", e);
			throw new StorageException(e);
		}
	}
	
	private boolean isDataRemoteFile(RemoteFile remoteFile) {
		return remoteFile.getClass().equals(MultichunkRemoteFile.class)
				|| (remoteFile.getClass().equals(TempRemoteFile.class) && remoteFile.getName().contains("multichunk"));
	}

	private String getRemoteFile(RemoteFile remoteFile) {
		String remoteFilePath = getRemoteFilePath(remoteFile.getClass());
		if (remoteFilePath != null)
			return remoteFilePath + remoteFile.getName();
		else
			return remoteFile.getName();
	}

	public String getRemoteFilePath(Class<? extends RemoteFile> remoteFile) {
		if (remoteFile.equals(MultichunkRemoteFile.class)) 
			return multichunksPath;
		else if (remoteFile.equals(DatabaseRemoteFile.class) || remoteFile.equals(CleanupRemoteFile.class)) 
			return databasesPath;
		else if (remoteFile.equals(ActionRemoteFile.class)) 
			return actionsPath;
		else if (remoteFile.equals(TransactionRemoteFile.class)) 
			return transactionsPath;
		else if (remoteFile.equals(TempRemoteFile.class)) 
			return tempPath;
		else 
			return null;
	}

	public boolean testTargetCanWrite() {
		try {
			String tempRemoteFilePath = "syncany-test-write";
			hybris.put(tempRemoteFilePath, new byte[]{0x01, 0x02, 0x03});
			logger.log(Level.INFO, "testTargetCanWrite: Success. Hybris has write access.");
			return true;
		} catch (Exception e) {
			logger.log(Level.INFO, "testTargetCanWrite: Hybris could not write.", e);
			return false;
		}
	}

	public boolean testTargetExists() {
		try {
			hybris.list();
			return true;
		} catch (Exception e) {
			logger.log(Level.INFO, "testTargetExists: Target exist test failed with exception.", e);
			return false;
		}
	}

	public boolean testTargetCanCreate() {
		try {
			return testTargetCanWrite();
		} catch (Exception e) {
			logger.log(Level.INFO, "testTargetCanCreate: Target can create test failed with exception.", e);
			return false;
		}
	}

	public boolean testRepoFileExists() {
		try {
			String repoRemoteFile = getRemoteFile(new SyncanyRemoteFile());
			List<String> repoFiles = hybris.rmds.rawList();
			
			if (repoFiles.contains(repoRemoteFile))
				return true;
			else
				return false;
		} catch (Exception e) {
			logger.log(Level.INFO, "testRepoFileExists: Retrieving repo file list does not exit.", e);
			return false;
		}
	}

	public static class HybrisReadAfterWriteConsistentFeatureExtension implements ReadAfterWriteConsistentFeatureExtension {

//		private HybrisTransferManager htm;
		
		public HybrisReadAfterWriteConsistentFeatureExtension(HybrisTransferManager hybrisTransferManager) {
//			this.htm = hybrisTransferManager;
		}
		
		public boolean exists(RemoteFile remoteFile) throws StorageException {
			// it should always return true because Hybris is strongly consistent by design
			return true;
//			String remoteFilePath = htm.getRemoteFile(remoteFile);
//			try {
//				if (remoteFile.getClass().equals(MultichunkRemoteFile.class))
//					return htm.hybris.list().contains(remoteFilePath);
//				else
//					return htm.hybris.rmds.rawList().contains(remoteFilePath);
//			} catch (HybrisException e) {
//				logger.log(Level.INFO, "exists: Hybris could not list.", e);
//				return false;
//			}
		}
	}
}
