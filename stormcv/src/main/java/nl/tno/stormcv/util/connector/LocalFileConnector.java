package nl.tno.stormcv.util.connector;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A {@link FileConnector} implementation used to interact with files on the local file system.
 * This connector is used for locations starting with file://
 * 
 * @author Corne Versloot
 *
 */
public class LocalFileConnector implements FileConnector{

	private static final long serialVersionUID = -7467881860874428816L;

	private static final String PREFIX = "file";
	private File localLocation;
	private ExtensionFilter filter = new ExtensionFilter();

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf) { }

	
	@Override
	public void moveTo(String location) throws IOException{
		try {
			this.localLocation = new File(new URI(location).getPath());
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
	}

	@Override
	public List<String> list() {
		List<String> listing = new ArrayList<String>();
		if(!localLocation.isDirectory()){
			if(filter.accept(localLocation, localLocation.getName())){
				listing.add(localLocation.toURI().toString());
			}
		}else for(String file : localLocation.list(filter)){
			listing.add(new File(localLocation, file).toURI().toString());
		}
		return listing;
	}

	@Override
	public String getProtocol() {
		return PREFIX;
	}

	@Override
	public File getAsFile() throws IOException{
		return localLocation;
	}

	@Override
	public LocalFileConnector setExtensions(String[] extensions) {
		this.filter.setExtensions(extensions);
		return this;
	}
	
	@Override
	public void copyFile(File localFile, boolean delete) throws IOException {
		Files.copy(localFile.toPath(), localLocation.toPath(), StandardCopyOption.REPLACE_EXISTING);
		localFile.delete();
	}
	
	@Override
	public FileConnector deepCopy() {
		FileConnector fl = new LocalFileConnector();
		if(this.localLocation != null)
			try {
				fl.moveTo(PREFIX+"://"+localLocation.getAbsolutePath());
			} catch (IOException e) {
				// TODO
			}
		return fl;
	}

	private class ExtensionFilter implements Serializable, FilenameFilter {

		private static final long serialVersionUID = 7715776887129530584L;
		private String[] extensions;
		
		@Override
		public boolean accept(File file, String name) {
			if(extensions == null) return true;
			for(String ext : extensions){
				if(name.endsWith(ext)) return true;
			}
			return false;
		}
		
		public void setExtensions(String[] extensions) {
			this.extensions = extensions;
		}
	}

}
