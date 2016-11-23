package org.syncany.plugins.hybris;

import java.io.File;
import java.util.HashMap;
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

	@Override
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

	@Override
	public void disconnect() throws StorageException {
		// Nothing
		logger.log(Level.INFO, "Hybris disconnect (not implemented)");
		// hybris.shutdown();
	}

	@Override
	public void init(boolean createIfRequired) throws StorageException {
		connect();
	}

	@Override
	public void download(RemoteFile remoteFile, File localFile) throws StorageException {
		connect();

		File tempFile = null;
		String remotePath = getRemoteFile(remoteFile);
		try {
			// Download
			byte[] data = hybris.get(remotePath);
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

	@Override
	public void upload(File localFile, RemoteFile remoteFile) throws StorageException {
		connect();

		String remotePath = getRemoteFile(remoteFile);
		try {
			byte[] data = FileUtils.readFileToByteArray(localFile);
			hybris.put(remotePath, data);
			logger.log(Level.FINE, "- Uploading: " + remotePath + " ...");
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Cannot upload " + localFile + " to " + remotePath, e);
			throw new StorageException(e);
		}
	}

	@Override
	public boolean delete(RemoteFile remoteFile) throws StorageException {
		connect();

		String remotePath = getRemoteFile(remoteFile);
		try {
			hybris.delete(remotePath);
			return true;
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Unable to delete remote file " + remotePath, e);
			throw new StorageException(e);
		}
	}

	@Override
	public void move(RemoteFile sourceFile, RemoteFile targetFile) throws StorageException {
		connect();

		String sourceRemotePath = getRemoteFile(sourceFile);
		String targetRemotePath = getRemoteFile(targetFile);
		try {
			byte[] data = hybris.get(sourceRemotePath);
			hybris.put(targetRemotePath, data);
			hybris.delete(sourceRemotePath);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Cannot move " + sourceRemotePath + " to " + targetRemotePath, e);
			throw new StorageMoveException(e);
		}
	}

	@Override
	public <T extends RemoteFile> Map<String, T> list(Class<T> remoteFileClass) throws StorageException {
		connect();

		try {
			// List
			List<String> objects = hybris.list();

			// Create RemoteFile objects
			Map<String, T> remoteFiles = new HashMap<String, T>();
			for (String object : objects) {
				String simpleRemoteName = object.substring(object.indexOf("-") + 1);

				if (simpleRemoteName.length() > 0) {
					try {
						T remoteFile = RemoteFile.createRemoteFile(simpleRemoteName, remoteFileClass);
						remoteFiles.put(simpleRemoteName, remoteFile);
					}
					catch (Exception e) {
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

	private String getRemoteFile(RemoteFile remoteFile) {
		String remoteFilePath = getRemoteFilePath(remoteFile.getClass());

		if (remoteFilePath != null)
			return remoteFilePath + remoteFile.getName();
		else
			return remoteFile.getName();
	}

	@Override
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

	@Override
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

	@Override
	public boolean testTargetExists() {
		try {
			hybris.list();
			return true;
		} catch (Exception e) {
			logger.log(Level.INFO, "testTargetExists: Target exist test failed with exception.", e);
			return false;
		}
	}

	@Override
	public boolean testTargetCanCreate() {
		try {
			return testTargetCanWrite();
		} catch (Exception e) {
			logger.log(Level.INFO, "testTargetCanCreate: Target can create test failed with exception.", e);
			return false;
		}
	}

	@Override
	public boolean testRepoFileExists() {
		try {
			String repoRemoteFile = getRemoteFile(new SyncanyRemoteFile());
			List<String> repoFiles = hybris.list();
			
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

		public HybrisReadAfterWriteConsistentFeatureExtension(HybrisTransferManager hybrisTransferManager) {}
		
		@Override
		public boolean exists(RemoteFile remoteFile) throws StorageException {
			// because Hybris is strongly consistent
			// TODO check that exists using hybris.list
			return true;
		}
	}
}
