package nl.tno.stormcv.grouping;

import java.util.ArrayList;
import java.util.List;

import nl.tno.stormcv.model.Feature;
import nl.tno.stormcv.model.serializer.FeatureSerializer;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;

/**
 * A custom grouping that functions as a <b>filter</b> as well as (optionally) a FieldGrouping. Only {@link Feature} with the specified
 * name will be passed through this grouping. It is possible to supply a set of Fields which will define what task the accepted 
 * tuple will be assigned to (similar to fieldGrouping). If no fields are provided the task is chosen randomly (like shuffleGrouping). 
 * 
 * @author Corne Versloot
 *
 */
public class FeatureGrouping implements CustomStreamGrouping {
	
	private static final long serialVersionUID = -2816800364074254046L;
	private String featureName;
	private int nameIndex = -1;
	private List<Integer> targetTasks;
	private ArrayList<Integer> indexes;

	/**
	 * Constructs a featureGrouping for the specified feature name. Features with this name will be routed
	 * according the the fields specified.
	 * @param featureName
	 * @param grouping
	 * @throws InstantiationException
	 */
	public FeatureGrouping(String featureName, Fields grouping) throws InstantiationException{
		this.featureName = featureName;
		FeatureSerializer serializer = new FeatureSerializer();
		Fields fields = serializer.getFields();
		indexes = new ArrayList<Integer>();
		for(String groupBy : grouping){
			for(int i=0; i<fields.size(); i++){
				if(groupBy.equals(fields.get(i))){
					indexes.add(i);
				}
				if(fields.get(i).equals(FeatureSerializer.NAME)){
					nameIndex = i;
				}
			}
		}
		if(nameIndex == -1)throw new InstantiationException("No index found for feature name: "+featureName);
		if(indexes.size() == 0) throw new InstantiationException("No field indexes found for provided grouping: "+grouping);
	}
	
	/**
	 * Constructs a featureGrouping for the specified feature name. Features with this name will be routed
	 * randomly.
	 * @param featureName
	 * @throws InstantiationException
	 */
	public FeatureGrouping(String featureName) throws InstantiationException{
		this.featureName = featureName;
		FeatureSerializer serializer = new FeatureSerializer();
		Fields fields = serializer.getFields();
		indexes = new ArrayList<Integer>();
		for(int i=0; i<fields.size(); i++){
			if(fields.get(i).equals(FeatureSerializer.NAME)){
				nameIndex = i;
			}
		}
		if(nameIndex == -1)throw new InstantiationException("No index found for feature name: "+featureName);
	}
	
	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		List<Integer> targets = new ArrayList<Integer>();
		if(!values.get(nameIndex).equals(featureName)) return targets;
		
		if(indexes != null && indexes.size() > 0){
			int hash = 0;
			for(int i : indexes){
				hash += values.get(i).hashCode();
			}
			targets.add(targetTasks.get(hash % targetTasks.size()));
		}else{
			int randI = (int)Math.floor(Math.random() * targetTasks.size());
			targets.add(targetTasks.get(randI));
		}
		return targets;
	}

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		this.targetTasks = targetTasks;
	}
	
}
