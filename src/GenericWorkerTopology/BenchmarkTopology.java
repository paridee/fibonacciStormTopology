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
  		    builder.setSpout("spout", new WorkTimeSpout(5000,Integer.parseInt(args[1])), 1);
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
