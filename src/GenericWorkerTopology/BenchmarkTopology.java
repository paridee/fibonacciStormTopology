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

public class BenchmarkTopology {
    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkTopology.class);
	public static void main(String[] args) throws Exception {
	    TopologyBuilder builder = new TopologyBuilder();
	    Config conf = new Config();
	    conf.setDebug(true);
	    conf.registerMetricsConsumer(PrometheusConsumer.class,1);
	    if (args != null && args.length > 1) {
  	   	  	conf.setNumWorkers(1);
  	   	  	conf.setMaxSpoutPending(5000);
  	   	  	int[] intervals	=	new int[24];
  	   	  	intervals[0]	=	60;
  	   	  	intervals[1]	=	65;
  	   	  	intervals[2]	=	70;
  	   	  	intervals[3]	=	75;
  	   	  	intervals[4]	=	80;
  	   	  	intervals[5]	=	75;
  	   	  	intervals[6]	=	70;
  	   	  	intervals[7]	=	50;
  	   	  	intervals[8]	=	45;
  	   	  	intervals[9]	=	42;
  	   	  	intervals[10]	=	39;
  	   	  	intervals[11]	=	35;
  	   	  	intervals[12]	=	30;
  	   	  	intervals[13]	=	20;
  	   	  	intervals[14]	=	10;
  	   	  	intervals[15]	=	20;
  	   	  	intervals[16]	=	17;
  	   	  	intervals[17]	=	30;
  	   	  	intervals[18]	=	43;
  	   	  	intervals[19]	=	45;
  	   	  	intervals[20]	=	35;
  	   	  	intervals[21]	=	31;
  	   	  	intervals[22]	=	10;
  	   	  	intervals[23]	=	30;
  	   	  	builder.setSpout("spout", new WorkTimeDynamicSpout(5000,intervals), 1);
  		    builder.setBolt("firststage", new IntermediateWorker(5000), 32).shuffleGrouping("spout");
  		    builder.setBolt("secondstage", new FinalWorker(), 32).shuffleGrouping("firststage");
  	   	  	StormSubmitter.submitTopology(args[0]+"", conf, builder.createTopology());
	    }
	    else {
		    builder.setSpout("spout", new WorkTimeSpout(5000,1000), 1);
		    builder.setBolt("firststage", new IntermediateWorker(5000), 32).shuffleGrouping("spout");
		    builder.setBolt("secondstage", new FinalWorker(), 32).shuffleGrouping("firststage");
	      conf.setMaxTaskParallelism(3);
	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("word-count", conf, builder.createTopology());

	      Thread.sleep(300000);

	      cluster.shutdown();
	      }
	}

}
