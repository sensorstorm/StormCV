package nl.tno.stormcv.example.drpc;

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
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This example shows the use of DRPC topologies. This topology indexes the faces images (example 2) and 
 * makes it possible to match other images with the faces using SIFT keypoints.
 * 
 * @author Corne Versloot
 *
 */

@SuppressWarnings("deprecation")
public class E8_DRPCTopology {
	
	public static void main(String[] args){
		// first some global (topology configuration)
		StormCVConfig conf = new StormCVConfig();

		conf.put(StormCVConfig.STORMCV_OPENCV_LIB, "mac64_opencv_java248.dylib");
				
		conf.setNumWorkers(5); // number of workers in the topology
		conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE); // indicates frames will be encoded as JPG throughout the topology (JPG is the default when not explicitly set)
		conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true); // True if Storm should timeout messages or not.
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS , 10); // The maximum amount of time given to the topology to fully process a message emitted by a spout (default = 30)
		conf.put(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT, false); // indicates if the spout must be fault tolerant; i.e. spouts do NOT! replay tuples on fail
		conf.put(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC, 30); // TTL (seconds) for all elements in all caches throughout the topology (avoids memory overload)
		conf.put(Config.NIMBUS_TASK_LAUNCH_SECS, 30);
		
		String userDir = System.getProperty("user.dir").replaceAll("\\\\", "/");
				
		List<String> prototypes = new ArrayList<String>();
		prototypes.add( "file://"+ userDir +"/resources/data" );
		
		// create a linear DRPC builder called 'match'
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("match");

		//add a FeatureMatchRequestOp that receives drpc requests
		builder.addBolt(new RequestBolt(new FeatureMatchRequestOp()), 1); 
		
		 // add two bolts that perform sift extraction (as used in other examples!)
		builder.addBolt(new SingleInputBolt(
			new FeatureExtractionOp("sift", FeatureDetector.SIFT, DescriptorExtractor.SIFT).outputFrame(false)
			), 1).shuffleGrouping();

		// add bolt that matches queries it gets with the prototypes it has loaded upon the prepare.
		// The prototypes are divided over the available tasks which means that each query has to be send to all tasks (use allGrouping)
		// the matcher only reports a match if at least 1 strong match has been found (can be set to 0)
		builder.addBolt(new SingleInputBolt(new PartialMatcher(prototypes, 0, 0.5f)), 2).allGrouping(); 
		
		// add a bolt that aggregates all the results it gets from the two matchers 
		builder.addBolt(new BatchBolt(new FeatureMatchResultOp(true)), 1).fieldsGrouping(new Fields(CVParticleSerializer.REQUESTID));
		
		// create local drpc server and cluster. Deploy the drpc topology on the cluster
		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("drpc-demo", conf, builder.createLocalTopology(drpc));
		
		// use all face images as queries (same images as loaded by the matcher!)
		File queryDir = new File(userDir +"/resources/data/");
		for(String img : queryDir.list()){
			if(!img.endsWith(".jpg")) continue; // to avoid reading non-image files
			// execute the drpc with the image as argument. Note that the execute blocks
			String matchesJson = drpc.execute("match", "file://"+userDir +"/resources/data/"+img);
			System.out.println(img+" : " + matchesJson);
		}
			
		cluster.shutdown();
		drpc.shutdown();
	}
}
