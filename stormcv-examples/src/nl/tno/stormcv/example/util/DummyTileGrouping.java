package nl.tno.stormcv.example.util;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class DummyTileGrouping implements CustomStreamGrouping {

	private static final long serialVersionUID = -4224490397406124809L;
	private List<Integer> targetTasks;
	private String[] accept;

	public DummyTileGrouping(String[] accept) {
		this.accept = accept;
	}	
	
	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		this.targetTasks = targetTasks;		
	}
	
	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		String streamId = (String)values.get(1);
		List<Integer> targets = new ArrayList<Integer>();
		for(String end : accept){
			if(streamId.endsWith(end)){
				int randI = (int)Math.floor(Math.random() * targetTasks.size());
				targets.add(targetTasks.get(randI));
				return targets;
			}
		}
		return targets; // might be empty which means the provided tuple is not send anywhere!
	}


}
