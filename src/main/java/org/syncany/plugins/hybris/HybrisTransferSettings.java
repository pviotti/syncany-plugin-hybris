package org.syncany.plugins.hybris;

import org.simpleframework.xml.Element;
import org.syncany.plugins.transfer.Setup;
import org.syncany.plugins.transfer.TransferSettings;

/**
 * @author PV
 */
public class HybrisTransferSettings extends TransferSettings {
	
	@Element(name = "propertyFile", required = true)
	@Setup(order = 1, description = "Property file")
	private String propertyFile;

	public String getPropertyFile() {
		return propertyFile;
	}
}
