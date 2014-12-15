package nl.tno.stormcv.grouping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

/**
 * This custom grouping initiates a fixed mapping from a components sourceId to a components targetId which will remain the 
 * same throughout the lifetime of the topology. This means that a single instance of a spout/bolt will always send the output
 * to the same receiving bolt. This can be useful when the exact load of the topology is known up front (for example when reading a fixed number of video streams).
 * Each instance of this grouping on the same edge in the topology will have the exact same mapping which is determined when the topology
 * is started.
 * 
 * Examples<br/>
 * 2 producers ids={1, 2} and 2 consumers ids={a, b} will have a mapping {1->a, 2->b}
 * 4 producers ids={1, 2, 3, 4} and 2 consumers ids={a, b} will have a mapping {1->a, 2->b, 3->a, 4->b}
 * 2 producers ids={1, 2} and 3 consumers ids={a, b, c} will have a mapping {1->a, 2->b} and consumer c will never be used!
 * 
 * Please note that this grouping is not robust, failure at some bolt will result in dataloss from one or spouts (i.e. some video's will no longer be processed)
 * 
 * @author Corne Versloot
 *
 */
public class SupervicedGrouping implements CustomStreamGrouping {

	private static final long serialVersionUID = -7868245310144592273L;
	private HashMap<Integer, Integer> idMapping = new HashMap<Integer, Integer>();

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> arg1) {
		List<Integer> result = new ArrayList<Integer>();
		if(idMapping.containsKey(taskId)){
			result.add(idMapping.get(taskId));
		}
		return result;
	}

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		List<Integer> sources = context.getComponentTasks(stream.get_componentId());
		for(int i=0; i<sources.size(); i++){
			idMapping.put(sources.get(i), targetTasks.get(i%targetTasks.size()));
		}
	}

}
