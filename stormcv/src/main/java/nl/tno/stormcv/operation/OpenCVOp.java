package nl.tno.stormcv.operation;

import java.io.IOException;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.util.NativeUtils;

/**
 * Abstract class containing basic functionality to load (custom) OpenCV libraries. The name of the library to be loaded
 * can be set though the libName function. If this name is not set it will attempt to find and load the right OpenCV 
 * library automatically. It is advised to set the name of the library to use. Operations that use OpenCV can extend this 
 * class and use its utility functions.
 * Operation.
 * 
 * @author Corne Versloot
 *
 */
public abstract class OpenCVOp<Output extends CVParticle> implements IOperation<Output>{

	private static final long serialVersionUID = -7758652109335765844L;

	private String libName;
	
	protected String getLibName(){
		return this.libName;
	}
	
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context) throws Exception{
		loadOpenCV(stormConf);
		this.prepareOpenCVOp(stormConf, context);
	}
	
	@SuppressWarnings("rawtypes")
	protected void loadOpenCV( Map stormConf) throws RuntimeException, IOException{
		this.libName = (String)stormConf.get(StormCVConfig.STORMCV_OPENCV_LIB);
		if(libName == null) NativeUtils.load();
		else NativeUtils.load(libName);
	}
	
	@SuppressWarnings("rawtypes")
	protected abstract void prepareOpenCVOp(Map stormConf, TopologyContext context) throws Exception;
	
}
