package nl.tno.stormcv.drpc;

import java.util.Map;

import nl.tno.stormcv.model.CVParticle;
import nl.tno.stormcv.model.Frame;
import backtype.storm.task.TopologyContext;

public class FeatureMatchResultOp implements IResultOp {

	private static final long serialVersionUID = -467562824142056924L;

	private String result;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void initBatch(Map stormConf, TopologyContext context) throws Exception {
		result = "No matching prototypes found!";
	}

	@Override
	public void processData(CVParticle particle) throws Exception {
		if(!(particle instanceof Frame)) return;
		Frame frame = (Frame) particle;
		if(frame.getMetadata().containsKey("strong_matches")){
			this.result = (String)frame.getMetadata().get("strong_matches");
		}
	}

	@Override
	public String getFinalResult() throws Exception {
		return result;
	}

}
