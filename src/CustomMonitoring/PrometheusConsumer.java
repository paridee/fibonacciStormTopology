package CustomMonitoring;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.starter.WordCountTopology.WordCount;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;

//import io.prometheus.client.exporter.MetricsServlet;

public class PrometheusConsumer implements IMetricsConsumer {
    private static final Logger LOG 	= 	LoggerFactory.getLogger(PrometheusConsumer.class);
    //private static final String PROMURL	=	"160.80.97.147:9091";
    private static final String PROMURL	=	"10.0.0.1:9091";
    
    //private static final CollectorRegistry registry = new CollectorRegistry(); 
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}



	@Override
	public void handleDataPoints(TaskInfo arg0, Collection<DataPoint> arg1) {
		Config conf 	= 	new Config();
		String topName	=	(String) conf.get(Config.TOPOLOGY_NAME);
		// TODO Auto-generated method stub
		 CollectorRegistry registry = new CollectorRegistry();
		 LOG.info("SONDA-PRE "+arg0.srcComponentId+" "+arg0.srcTaskId+" "+arg0.srcWorkerHost+" "+arg0.srcWorkerPort+" "+arg0.timestamp+" "+arg0.updateIntervalSecs);
		 LOG.info("SONDA-PRE2 "+arg1.size());
		 Iterator<DataPoint> it	=	arg1.iterator();
		 while(it.hasNext()){
			 DataPoint dp	=	it.next();
			 LOG.info("SONDA-INSIDE "+dp.name+" "+dp.value.toString()+" "+dp.value.getClass());
			 if(dp.value instanceof HashMap){
				 Iterator innerIt	=	((HashMap)dp.value).keySet().iterator();
				 while(innerIt.hasNext()){
					 Object innerKey	=	innerIt.next();
					 Object innerValue	=	((HashMap)dp.value).get(innerKey);
					 LOG.info("SONDA-INSIDE-INSIDE "+innerKey.toString()+" "+innerValue.toString()+" "+innerValue.getClass());	
					 String metricName	=	"_storm_"+dp.name+"_"+innerKey.toString();
					 metricName	=	metricName.replace('-', '_');
					 metricName	=	metricName.replace('/', '_');
					 //if(metricName.length()>=30){
						// metricName	=   metricName.substring(0, 30);
					 //}
					 double gaugeValue	=	-1;
					 if(innerValue instanceof Long){
						 gaugeValue	=	((Long)innerValue).doubleValue();
					 }
					 else if(innerValue instanceof Integer){
						 gaugeValue	=	((Integer)innerValue).doubleValue();
					 }
					 else if(innerValue instanceof Double){
						 gaugeValue	=	((Double)innerValue);
					 }
					 LOG.info("SONDA-INSIDE-INSIDE gauge name "+"storm_"+dp.name+"_"+innerKey.toString()+" innervalue type "+innerValue.getClass());
					String[] topFeat	=	new String[1];
					topFeat[0]	=	topName+"";
					String[] labelNames	=	new String[1];
					labelNames[0]			=	"topology";
					 Gauge duration = Gauge.build()
						     .name(metricName)
						     .help(metricName)
						     .labelNames(labelNames)
						     .register(registry);
					 
					 Gauge.Child gaugeChild	=	new Gauge.Child();
					 gaugeChild.set(gaugeValue);
					 duration.setChild(gaugeChild, topFeat);				 
					 LOG.info("SONDA-INSIDE-INSIDE checkup "+gaugeValue+" "+duration.toString()+" "+registry.toString());
						
				 }
			 }
			 String metricName	=	dp.name;
			 metricName	=	metricName.replace('-', '_');
			 metricName	=	metricName.replace('/', '_');
			 if(dp.value instanceof Double){
					String[] topFeat	=	new String[1];
					topFeat[0]	=	topName+"";
					String[] labelNames	=	new String[1];
					labelNames[0]			=	"topology";
				 Gauge duration = Gauge.build()
					     .name(metricName)
					     .help(metricName)
					     .labelNames(labelNames)
					     .register(registry);
				 
				 Gauge.Child gaugeChild	=	new Gauge.Child();
				 gaugeChild.set((double)dp.value);
				 duration.setChild(gaugeChild, topFeat);
					 LOG.info("SONDA-INSIDE-ELSE gauge name "+"storm_"+dp.name);
			 }
			 else if(dp.value instanceof Long){
					String[] topFeat	=	new String[1];
					topFeat[0]	=	topName+"";
					String[] labelNames	=	new String[1];
					labelNames[0]			=	"topology";
				 Gauge duration = Gauge.build()
					     .name(metricName)
					     .help(metricName)
					     .labelNames(labelNames)
					     .register(registry);
				 
				 Gauge.Child gaugeChild	=	new Gauge.Child();
				 gaugeChild.set(((Long)dp.value).doubleValue());
				 duration.setChild(gaugeChild, topFeat);
			 }
			 else if(dp.value instanceof Integer){
					String[] topFeat	=	new String[1];
					topFeat[0]	=	topName+"";
					String[] labelNames	=	new String[1];
					labelNames[0]			=	"topology";
				 Gauge duration = Gauge.build()
					     .name(metricName)
					     .help(metricName)
					     .labelNames(labelNames)
					     .register(registry);
				 
				 Gauge.Child gaugeChild	=	new Gauge.Child();
				 gaugeChild.set(((Integer)dp.value).doubleValue());
				 duration.setChild(gaugeChild, topFeat);
			 }
			 else{
				 LOG.info("SONDA ELSEBLOCK "+dp.value.getClass());
			 }
		 }

		   PushGateway pg = new PushGateway(PROMURL);
		   try {
			   pg.pushAdd(registry, "stormMetrics");
			//pg.pushAdd(registry, "stormMetrics",arg0.srcComponentId+"_"+arg0.srcTaskId+"_"+arg0.srcWorkerHost+"_"+arg0.srcWorkerPort);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.warn("SONDA######"+e.getMessage());
			//e.printStackTrace();
		}
		LOG.info("############SONDA!!! sent to prometheus ");
	
}
	
	@Override
	public void prepare(Map arg0, Object arg1, TopologyContext arg2, IErrorReporter arg3) {
		/* TODO Auto-generated method stub
		org.eclipse.jetty.server.Server server	=	new org.eclipse.jetty.server.Server(PORT);
		ServletContextHandler			context	=	new ServletContextHandler();
		context.setContextPath("/");
		server.setHandler(context);
	//	context.addServlet(new ServletHolder(
		//	      new MetricsServlet()), "/metrics");
			  // Put your application setup code here.
			  try {
				server.start();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}*/
	}

}
