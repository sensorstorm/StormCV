package nl.tno.stormcv.drpc;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.opencv.features2d.DescriptorExtractor;
import org.opencv.features2d.FeatureDetector;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.bolt.SingleInputBolt;
import nl.tno.stormcv.drpc.BatchBolt;
import nl.tno.stormcv.drpc.RequestBolt;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.CVParticleSerializer;
import nl.tno.stormcv.operation.FeatureExtractionOp;
import nl.tno.stormcv.util.connector.S3Connector;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.tuple.Fields;

@SuppressWarnings("deprecation")
public class StormCVDRPC {
	
	public static void main(String[] args){
		// first some global (topology configuration)
		StormCVConfig conf = new StormCVConfig();

		conf.put(StormCVConfig.STORMCV_OPENCV_LIB, "mac64_opencv_java248.dylib");
				
		conf.setNumWorkers(4); // number of workers in the topology
		conf.setMaxSpoutPending(32); // maximum un-acked/un-failed frames per spout (spout blocks if this number is reached)
		conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE); // indicates frames will be encoded as JPG throughout the topology (JPG is the default when not explicitly set)
		conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true); // True if Storm should timeout messages or not.
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS , 10); // The maximum amount of time given to the topology to fully process a message emitted by a spout (default = 30)
		conf.put(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT, false); // indicates if the spout must be fault tolerant; i.e. spouts do NOT! replay tuples on fail
		conf.put(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC, 30); // TTL (seconds) for all elements in all caches throughout the topology (avoids memory overload)
		conf.put(Config.NIMBUS_TASK_LAUNCH_SECS, 30);
		String userDir = System.getProperty("user.dir").replaceAll("\\\\", "/");
				
		List<String> prototypes = new ArrayList<String>();
		//prototypes.add( "file://"+ userDir +"/resources/data/prototypes" );
		prototypes.add("file:///Users/verslootca/Projects/StormCV/corne_test/");
		
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("protomatch");
		builder.addBolt(new RequestBolt(new FeatureMatchRequestOp()), 1);
		//builder.addBolt(new SingleInputBolt(new FeatureMatcherOp(prototypes, 10, 0.5f)), 2).shuffleGrouping();
		builder.addBolt(new SingleInputBolt(
			new FeatureExtractionOp("sift", FeatureDetector.SIFT, DescriptorExtractor.SIFT).outputFrame(false)
			), 2).shuffleGrouping();
		builder.addBolt(new SingleInputBolt(new PartialMatcher(prototypes, 1, 0.5f)), 2).allGrouping();
		builder.addBolt(new BatchBolt(new FeatureMatchResultOp(true)), 1).fieldsGrouping(new Fields(CVParticleSerializer.REQUESTID));
		
		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();

		cluster.submitTopology("drpc-demo", conf, builder.createLocalTopology(drpc));
		try {Thread.sleep(30000);} catch (InterruptedException e) {		}
		
		int count = 0;
		File queryDir = new File("/Users/verslootca/Projects/StormCV/corne_test/");
		for(String img : queryDir.list()){
			if(!img.endsWith(".jpg")) continue;
			System.err.println(img+" ("+count+") : " + drpc.execute("protomatch", "file:///Users/verslootca/Projects/StormCV/corne_test/"+img));
			count++;
		}
			
		cluster.shutdown();
		drpc.shutdown();
	}
}
