package nl.tno.stormcv.example;

import java.util.ArrayList;
import java.util.List;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.batcher.SlidingWindowBatcher;
import nl.tno.stormcv.bolt.BatchInputBolt;
import nl.tno.stormcv.bolt.SingleInputBolt;
import nl.tno.stormcv.fetcher.FileFrameFetcher;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.model.serializer.FrameSerializer;
import nl.tno.stormcv.operation.DrawFeaturesOp;
import nl.tno.stormcv.operation.FeatureExtractionOp;
import nl.tno.stormcv.operation.HaarCascadeOp;
import nl.tno.stormcv.operation.ISingleInputOperation;
import nl.tno.stormcv.operation.MjpegStreamingOp;
import nl.tno.stormcv.operation.ScaleImageOp;
import nl.tno.stormcv.operation.SequentialFrameOp;
import nl.tno.stormcv.spout.CVParticleSpout;

import org.opencv.features2d.DescriptorExtractor;
import org.opencv.features2d.FeatureDetector;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class E4_SequentialFeaturesTopology {

	public static void main(String[] args){
		
		// first some global (topology configuration)
		StormCVConfig conf = new StormCVConfig();
		
		/**
		 * Sets the OpenCV library to be used which depends on the system the topology is being executed on
		 */
		conf.put(StormCVConfig.STORMCV_OPENCV_LIB, "mac64_opencv_java248.dylib");
		
		conf.setNumWorkers(8); // number of workers in the topology
		conf.setMaxSpoutPending(6); // maximum un-acked/un-failed frames per spout (spout blocks if this number is reached)
		conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE); // indicates frames will be encoded as JPG throughout the topology (JPG is the default when not explicitly set)
		conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true); // True if Storm should timeout messages or not.
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS , 10); // The maximum amount of time given to the topology to fully process a message emitted by a spout (default = 30)
		conf.put(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT, false); // indicates if the spout must be fault tolerant; i.e. spouts do NOT! replay tuples on fail
		conf.put(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC, 30); // TTL (seconds) for all elements in all caches throughout the topology (avoids memory overload)
		
		String userDir = System.getProperty("user.dir").replaceAll("\\\\", "/");
		// create a list with files to be processed, in this case just one. Multiple files will be spread over the available spouts
		List<String> files = new ArrayList<String>();
		files.add( "file://"+ userDir+"/resources/data/"); // will process all video files in this directory (i.e. two files)

		// specify the list with SingleInputOperations to be executed sequentially by the 'fat' bolt
		@SuppressWarnings("rawtypes")
		List<ISingleInputOperation> operations = new ArrayList<ISingleInputOperation>();
		operations.add(new HaarCascadeOp("face", "lbpcascade_frontalface.xml") );
		operations.add(new FeatureExtractionOp("sift", FeatureDetector.SIFT, DescriptorExtractor.SIFT));
		
		int frameSkip = 13; 
		
		// now create the topology itself (spout -> scale -> fat[face detection & sift] -> drawer -> streamer)
		TopologyBuilder builder = new TopologyBuilder();
		 // just one spout reading video files, extracting 1 frame out of 25 (i.e. 1 per second)
		builder.setSpout("spout", new CVParticleSpout( new FileFrameFetcher(files).frameSkip(frameSkip) ), 1 );
		
		// add bolt that scales frames down to 25% of the original size 
		builder.setBolt("scale", new SingleInputBolt( new ScaleImageOp(0.25f)), 1)
			.shuffleGrouping("spout");
		
		// three 'fat' bolts containing a SequentialFrameOperation will will emit a Frame object containing the detected features
		builder.setBolt("fat_features", new SingleInputBolt( new SequentialFrameOp(operations).outputFrame(true).retainImage(true)), 3)
			.shuffleGrouping("scale");
		
		// simple bolt that draws Features (i.e. locations of features) into the frame
		builder.setBolt("drawer", new SingleInputBolt(new DrawFeaturesOp()), 1)
			.shuffleGrouping("fat_features");
		
		// add bolt that creates a webservice on port 8558 enabling users to view the result
		builder.setBolt("streamer", new BatchInputBolt(
				new SlidingWindowBatcher(2, frameSkip).maxSize(6), // note the required batcher used as a buffer and maintains the order of the frames
				new MjpegStreamingOp().port(8558).framerate(5)).groupBy(new Fields(FrameSerializer.STREAMID))
			, 1)
			.shuffleGrouping("drawer");
		
		try {
			
			// run in local mode
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology( "fatfeature", conf, builder.createTopology() );
			Utils.sleep(120*1000); // run for two minutes and then kill the topology
			cluster.shutdown();
			System.exit(1);
			
			// run on a storm cluster
			// StormSubmitter.submitTopology("some_topology_name", conf, builder.createTopology());
		} catch (Exception e){
			e.printStackTrace();
		}
	}
}
