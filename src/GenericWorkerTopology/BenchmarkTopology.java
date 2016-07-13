package GenericWorkerTopology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import CustomMonitoring.PrometheusConsumer;
import generators.DynamicIntegerGenerator;

public class BenchmarkTopology {
    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkTopology.class);
	public static void main(String[] args) throws Exception {
	    TopologyBuilder builder = new TopologyBuilder();
	    Config conf = new Config();
	    conf.setDebug(true);
	    conf.registerMetricsConsumer(PrometheusConsumer.class,1);
	    
	   	  	int[] intervals	=	new int[24];
	   	  	intervals[0]	=	30;
	   	  	intervals[1]	=	32;
	   	  	intervals[2]	=	35;
	   	  	intervals[3]	=	37;
	   	  	intervals[4]	=	40;
	   	  	intervals[5]	=	37;
	   	  	intervals[6]	=	35;
	   	  	intervals[7]	=	25;
	   	  	intervals[8]	=	23;
	   	  	intervals[9]	=	21;
	   	  	intervals[10]	=	19;
	   	  	intervals[11]	=	17;
	   	  	intervals[12]	=	15;
	   	  	intervals[13]	=	10;
	   	  	intervals[14]	=	5;
	   	  	intervals[15]	=	10;
	   	  	intervals[16]	=	8;
	   	  	intervals[17]	=	15;
	   	  	intervals[18]	=	22;
	   	  	intervals[19]	=	24;
	   	  	intervals[20]	=	17;
	   	  	intervals[21]	=	15;
	   	  	intervals[22]	=	5;
	   	  	intervals[23]	=	15;

	   	  	int[] deltas1	=	new int[4];
	   	  	deltas1[0]			=	2;
	   	  	deltas1[1]			=	2;
	   	  	deltas1[2]			=	2;
	   	  	deltas1[3]			=	2;
	   	  	
	   	  	int[] basev1		=	new int[4];
	   	  	basev1[0]			=	32;
	   	  	basev1[1]			=	33;
	   	  	basev1[2]			=	34;
	   	  	basev1[3]			=	30;
	   	  	
	   	  	int[] deltas2	=	new int[4];
	   	  	deltas2[0]			=	2;
	   	  	deltas2[1]			=	2;
	   	  	deltas2[2]			=	2;
	   	  	deltas2[3]			=	2;
	   	  	
	   	  	int[] basev2		=	new int[4];
	   	  	basev2[0]			=	30;
	   	  	basev2[1]			=	30;
	   	  	basev2[2]			=	30;
	   	  	basev2[3]			=	34;
	   	  	
	   	  	int[] deltas3	=	new int[4];
	   	  	deltas3[0]			=	2;
	   	  	deltas3[1]			=	2;
	   	  	deltas3[2]			=	2;
	   	  	deltas3[3]			=	2;
	   	  	
	   	  	int[] basev3		=	new int[4];
	   	  	basev3[0]			=	33;
	   	  	basev3[1]			=	31;
	   	  	basev3[2]			=	31;
	   	  	basev3[3]			=	30;
	   	  	
	   	  	DynamicIntegerGenerator gen1	=	new DynamicIntegerGenerator(basev1,deltas1);
	   	  	DynamicIntegerGenerator gen2	=	new DynamicIntegerGenerator(basev2,deltas2);
	   	  	DynamicIntegerGenerator gen3	=	new DynamicIntegerGenerator(basev3,deltas3);
	   	  	builder.setSpout("spout", new WorkTimeDynamicSpout(intervals,gen1), 1);
	    
	    if (args != null && args.length > 1) {
  	   	  	conf.setNumWorkers(1);
  	   	  	conf.setMaxSpoutPending(5000);

  		    builder.setBolt("firststage", new IntermediateWorker(gen2), 1).shuffleGrouping("spout").setNumTasks(32);
  		    builder.setBolt("secondstage", new IntermediateWorker(gen3), 1).shuffleGrouping("firststage").setNumTasks(32);
  		    builder.setBolt("thirdstage", new FinalWorker(), 1).shuffleGrouping("secondstage").setNumTasks(32);
  	   	  	StormSubmitter.submitTopology(args[0]+"", conf, builder.createTopology());
	    }
	    else {
  	   	  	//builder.setSpout("spout", new WorkTimeDynamicSpout(intervals,gen1), 1);
  		    builder.setBolt("firststage", new IntermediateWorker(gen2), 1).shuffleGrouping("spout");
  		    builder.setBolt("secondstage", new IntermediateWorker(gen3), 1).shuffleGrouping("firststage");
  		    builder.setBolt("thirdstage", new FinalWorker(), 1).shuffleGrouping("secondstage");
	      conf.setMaxTaskParallelism(3);
	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("word-count", conf, builder.createTopology());

	      Thread.sleep(3600000);

	      cluster.shutdown();
	      }
	}

}
