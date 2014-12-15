package nl.tno.stormcv.example;

import java.util.ArrayList;
import java.util.List;

import nl.tno.stormcv.StormCVConfig;
import nl.tno.stormcv.bolt.SingleInputBolt;
import nl.tno.stormcv.fetcher.ImageFetcher;
import nl.tno.stormcv.model.Frame;
import nl.tno.stormcv.operation.DrawFeaturesOp;
import nl.tno.stormcv.operation.HaarCascadeOp;
import nl.tno.stormcv.operation.ROIExtractionOp;
import nl.tno.stormcv.spout.CVParticleSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class E2_FacedetectionTopology {

	public static void main(String[] args){
		// first some global (topology configuration)
		StormCVConfig conf = new StormCVConfig();

		/**
		 * Sets the OpenCV library to be used which depends on the system the topology is being executed on
		 */
		conf.put(StormCVConfig.STORMCV_OPENCV_LIB, "mac64_opencv_java248.dylib");
		//conf.put(StormCVConfig.STORMCV_OPENCV_LIB, "win64_opencv_java248.dll");
		
		conf.setNumWorkers(3); // number of workers in the topology
		conf.setMaxSpoutPending(32); // maximum un-acked/un-failed frames per spout (spout blocks if this number is reached)
		conf.put(StormCVConfig.STORMCV_FRAME_ENCODING, Frame.JPG_IMAGE); // indicates frames will be encoded as JPG throughout the topology (JPG is the default when not explicitly set)
		conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true); // True if Storm should timeout messages or not.
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS , 10); // The maximum amount of time given to the topology to fully process a message emitted by a spout (default = 30)
		conf.put(StormCVConfig.STORMCV_SPOUT_FAULTTOLERANT, false); // indicates if the spout must be fault tolerant; i.e. spouts do NOT! replay tuples on fail
		conf.put(StormCVConfig.STORMCV_CACHES_TIMEOUT_SEC, 30); // TTL (seconds) for all elements in all caches throughout the topology (avoids memory overload)

		
		String userDir = System.getProperty("user.dir").replaceAll("\\\\", "/");
		
		// create a list with files to be processed, in this case just one. Multiple files will be spread over the available spouts
		List<String> files = new ArrayList<String>();
		files.add( "file://"+ userDir +"/resources/data/" ); // adding a directory will process all files within the directory (also subidrs)

		// now create the topology itself (spout -> facedetection --> drawer)
		TopologyBuilder builder = new TopologyBuilder();
		 // just one spout reading images from a directory, sleeping 100ms after each file was read
		builder.setSpout("spout", new CVParticleSpout( new ImageFetcher(files).sleepTime(100) ), 1 );
		
		// one bolt with a HaarCascade classifier with the lbpcascade_frontalface model to detecting faces. This operation outputs a Frame including the Features with detected faces
		builder.setBolt("face_detect", new SingleInputBolt(
			new HaarCascadeOp("face", "lbpcascade_frontalface.xml")
					.outputFrame(true)
			), 1)
			.shuffleGrouping("spout");
				
		// The bounding boxes of the Face Feature are extracted from the Frame and emitted as separate frames
		builder.setBolt("face_extraction", new SingleInputBolt(
				new ROIExtractionOp("face").spacing(25)
			), 1)
			.shuffleGrouping("face_detect");
				
		// simple bolt that draws Features (i.e. locations of features) into the frame and writes the frame to the local file system at /output/facedetections
		builder.setBolt("drawer", new SingleInputBolt(new DrawFeaturesOp().destination("file://"+userDir+ "/output/facedetections/")), 1)
			.shuffleGrouping("face_extraction");
		
		try {
			// run in local mode
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology( "facedetection", conf, builder.createTopology() );
			Utils.sleep(300*1000); // run for some time and then kill the topology
			cluster.shutdown();
			System.exit(1);
			
			// run on a storm cluster
			// StormSubmitter.submitTopology("some_topology_name", conf, builder.createTopology());
		} catch (Exception e){
			e.printStackTrace();
		}
	}

}
