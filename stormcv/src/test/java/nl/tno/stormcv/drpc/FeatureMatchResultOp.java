package nl.tno.stormcv.drpc;

import java.util.Map;

import nl.tno.stormcv.drpc.IResultOp;
import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Frame;
import backtype.storm.task.TopologyContext;

public class FeatureMatchResultOp implements IResultOp {

	private static final long serialVersionUID = -467562824142056924L;

	private String result;

	private boolean prettyJson;
	
	public FeatureMatchResultOp(boolean prettyJson) {
		this.prettyJson = prettyJson;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void initBatch(Map stormConf, TopologyContext context) throws Exception {
		result = new String();
	}

	@Override
	public void processData(CVParticle particle) throws Exception {
		if(!(particle instanceof Frame)) return;
		Frame frame = (Frame) particle;
		if(frame.getMetadata().containsKey("strong_matches")){
			this.result += (String)frame.getMetadata().get("strong_matches");
		}
	}

	@Override
	public String getFinalResult() throws Exception {
		if(prettyJson){
			result = result.replaceAll(";" , ",\r\n\t");
			return "{\r\n\t"+result.trim()+"\r\n}";
		}else{
			result.replaceAll(";",",");
			return "{"+result.trim()+"}";
		}
	}

}
