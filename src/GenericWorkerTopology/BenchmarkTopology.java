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
  	   	  	intervals[0]	=	120;
  	   	  	intervals[1]	=	130;
  	   	  	intervals[2]	=	140;
  	   	  	intervals[3]	=	150;
  	   	  	intervals[4]	=	160;
  	   	  	intervals[5]	=	150;
  	   	  	intervals[6]	=	140;
  	   	  	intervals[7]	=	100;
  	   	  	intervals[8]	=	90;
  	   	  	intervals[9]	=	85;
  	   	  	intervals[10]	=	78;
  	   	  	intervals[11]	=	70;
  	   	  	intervals[12]	=	60;
  	   	  	intervals[13]	=	40;
  	   	  	intervals[14]	=	20;
  	   	  	intervals[15]	=	40;
  	   	  	intervals[16]	=	35;
  	   	  	intervals[17]	=	60;
  	   	  	intervals[18]	=	85;
  	   	  	intervals[19]	=	90;
  	   	  	intervals[20]	=	70;
  	   	  	intervals[21]	=	63;
  	   	  	intervals[22]	=	20;
  	   	  	intervals[23]	=	60;
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
