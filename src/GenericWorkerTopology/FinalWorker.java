package GenericWorkerTopology;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FinalWorker extends BaseBasicBolt {
	private static final Logger LOG = LoggerFactory.getLogger(FinalWorker.class);
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		int intFib	=	input.getInteger(0);
		LOG.info("### ---> Going to elaborate Fib("+intFib+")");
		Long start	=	System.currentTimeMillis();
		Long res	=	fibonacci(intFib); 
		start		=	System.currentTimeMillis()-start;
		LOG.info("### ---> Elaboration finished "+res+" in "+start+" ms");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}

    public static long fibonacci(long i) {
    	/* F(i) non e` definito per interi i negativi! */
    	if (i == 0) return 0;
		else if (i == 1) return 1;
		else return fibonacci(i-1) + fibonacci(i-2);
    }
    
}
