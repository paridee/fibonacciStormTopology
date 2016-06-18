package GenericWorkerTopology;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.starter.spout.RandomIntegerSpout;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkTimeSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(WorkTimeSpout.class);
    private SpoutOutputCollector collector;
    private long msgId = 0;
    private int maxValue;
    private int genInterval;
    
    public WorkTimeSpout(int maxValue,int genInterval){
    	super();
    	this.maxValue		=	maxValue;
    	this.genInterval	=	genInterval;
    }
    
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
        Utils.sleep(genInterval);
        collector.emit(new Values(IntegerGenerator.generateValue(), System.currentTimeMillis() - (24 * 60 * 60 * 1000), ++msgId), msgId);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("value", "ts", "msgid"));
	}
	
    @Override
    public void ack(Object msgId) {
        LOG.debug("Got ACK for msgId : " + msgId);
    }

    @Override
    public void fail(Object msgId) {
        LOG.debug("Got FAIL for msgId : " + msgId);
    }

}
