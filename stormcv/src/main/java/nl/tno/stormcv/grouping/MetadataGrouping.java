package nl.tno.stormcv.grouping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;

/**
 * A CustomGrouping used to filter and/or route StormCV objects based on metadata they contain. This grouping can operate in two modes:
 * <ul>
 * <li><i>Constructed with one or more metadata keys</i> will route tuples based on the hashmod based on the combined values for those keys.
 * This means that data with the same metadata values are routed to the same task</li>
 * <li><i>Constructed with a map containing metadata key-value pairs to accept</i>. If a tuple does not contain one of the specified key-value
 * pairs it will not be routed anywhere (i.e. will be removed from the stream). In addition it is possible to specify if accepted tuples must be routed
 * randomly or using the hashmod method. The first means that accepted tuples are routed to randomly chosen tasks, the second method will choose
 * the task based on the hashmod of the <i>combined hash from accepted values</i> within the metadata map.</li>
 * </ul>
 * 
 * This grouping can be used for example to route {@link Frame}'s that contains for example classlabel metdata ("label" -> {"a", "b", "c"}) to the same bolt.
 * When there are two recieving taks it might be the case that all a's and b's are routed to task 1 and all c'c are routed to task 2 (however the hashmod function chooses) 
 * 
 * @author Corne Versloot
 *
 */
public class MetadataGrouping implements CustomStreamGrouping {

	private static final long serialVersionUID = 7145628575462154690L;
	private boolean hashmod;
	private Map<String, Object[]> accept;
	private String[] keysToHash;
	private List<Integer> targetTasks;
	private int mdIndex;

	public MetadataGrouping(Map<String, Object[]> accept, boolean hashmod) throws InstantiationException{
		this.accept = accept;
		this.hashmod = hashmod;
		Fields fields = new FrameSerializer().getFields();
		for(int i=0; i<fields.size(); i++){
			if(fields.get(i).equals(CVParticleSerializer.METADATA)){
				mdIndex = i;
				break;
			}
		}
		if(mdIndex == -1) throw new InstantiationException("Unable to find "+CVParticleSerializer.METADATA+ " field in CVParticle tuple representation!");
	}
	
	public MetadataGrouping(String[] keysToHash) throws InstantiationException{
		this.keysToHash = keysToHash;
		this.hashmod = true;
		Fields fields = new FrameSerializer().getFields();
		for(int i=0; i<fields.size(); i++){
			if(fields.get(i).equals(CVParticleSerializer.METADATA)){
				mdIndex = i;
				break;
			}
		}
		if(mdIndex == -1) throw new InstantiationException("Unable to find "+CVParticleSerializer.METADATA+ " field in CVParticle tuple representation!");
	}
	
	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId streamId, List<Integer> targetTasks) {
		this.targetTasks = targetTasks;
	}
	
	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		if(accept != null) return chooseTasksForMap(taskId, values);
		else return chooseTasksForKeys(taskId, values);
	}

	@SuppressWarnings("unchecked")
	private List<Integer> chooseTasksForKeys(int taskId, List<Object> values) {
		List<Integer> targets = new ArrayList<Integer>();
		Map<String, Object> metadata = (Map<String, Object>)values.get(mdIndex);
		int hash = 0;
		for(String key : keysToHash){
			if(!metadata.containsKey(key)) continue;
			hash += metadata.get(key).hashCode();
		}
		if(hash != 0) targets.add( targetTasks.get(Math.abs( hash ) % targetTasks.size()) );
		return targets;
	}

	@SuppressWarnings("unchecked")
	private List<Integer> chooseTasksForMap(int taskId, List<Object> values){
		List<Integer> targets = new ArrayList<Integer>();
		Map<String, Object> metadata = (Map<String, Object>)values.get(mdIndex);
		int hash = 0;
		for(String key : accept.keySet()){
			if(!metadata.containsKey(key)) continue;
			Object mdValue = metadata.get(key);
			for(Object acc : accept.get(key)){
				if(acc.equals(mdValue)){
					if(hashmod) hash += mdValue.hashCode();
					else{
						targets.add( targetTasks.get( (int)Math.floor(Math.random() * targetTasks.size()) ) );
						return targets;
					}
				}
			}
		}
		if(hash != 0) targets.add( targetTasks.get(Math.abs( hash ) % targetTasks.size()) );
		return targets;
	}

}
