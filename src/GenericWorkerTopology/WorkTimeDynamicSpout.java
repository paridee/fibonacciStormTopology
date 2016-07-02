package GenericWorkerTopology;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import generators.IntegerGenerator;
import generators.StaticIntegerGenerator;

public class WorkTimeDynamicSpout extends BaseRichSpout {
    /**
	 * 
	 */
	private static final long serialVersionUID = 6983495317915896224L;
	private static final Logger LOG = LoggerFactory.getLogger(WorkTimeDynamicSpout.class);
    private SpoutOutputCollector collector;
    private long msgId = 0;
    private int[] intervals;
    IntegerGenerator generator;
    
    public WorkTimeDynamicSpout(int[] genInterval,IntegerGenerator generator){
    	super();
    	this.intervals		=	genInterval;
    	this.generator		=	generator;
    }
    
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		Date	date	=	new Date();
		Calendar calendar = GregorianCalendar.getInstance(); // creates a new calendar instance
		calendar.setTime(date);   // assigns calendar to given date 
		int hourNow		=	calendar.get(Calendar.HOUR_OF_DAY); // gets hour in 24h format
		int minutesNow	=	calendar.get(Calendar.MINUTE);
		double begin	=	intervals[hourNow];
		double end		=	intervals[(hourNow+1)%24];
		double sleepVal	=	begin+((((double)(minutesNow))/60)*((double)(end-begin)));
        //LOG.info("Current time is ");
		Utils.sleep((int)sleepVal);
        collector.emit(new Values(this.generator.generateValue(), System.currentTimeMillis() - (24 * 60 * 60 * 1000), ++msgId), msgId);
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
