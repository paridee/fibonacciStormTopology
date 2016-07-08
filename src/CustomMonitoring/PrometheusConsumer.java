package CustomMonitoring;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

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
    HashMap<String,Gauge> gauges	=	new HashMap<String,Gauge>();
    
    //private static final CollectorRegistry registry = new CollectorRegistry(); 
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}
	
	
	public static void sendEmail(String text,String obj,String address){
	    Properties props = new Properties();
	    props.put("mail.smtp.host", "secure.alien8.it");
	    props.put("mail.smtp.socketFactory.port", "465");
	    props.put("mail.smtp.socketFactory.class",
	            "javax.net.ssl.SSLSocketFactory");
	    props.put("mail.smtp.auth", "true");
	    props.put("mail.smtp.port", "465"); 
	    Session session = Session.getDefaultInstance(props,
	        new javax.mail.Authenticator() {
	                            @Override
	            protected PasswordAuthentication getPasswordAuthentication() {
	                return new PasswordAuthentication("noreply@eclshop.tv","193ofQf279");
	            }
	        });

	    try {

	        Message message = new MimeMessage(session);
	        Address anAddr	=	new InternetAddress("noreply@eclshop.tv");
	        message.setFrom(anAddr);
	        message.setRecipients(Message.RecipientType.TO,
	                InternetAddress.parse(address));
	        message.setSubject(obj);
	        message.setText(text);
	        
	        Transport.send(message);

	        System.out.println("Sending result, done.");

	    } catch (MessagingException e) {
	        throw new RuntimeException(e);
	    }
}



	@Override
	public void handleDataPoints(TaskInfo arg0, Collection<DataPoint> arg1) {
		Config conf 	= 	new Config();
		String topName	=	(String) conf.get(Config.TOPOLOGY_NAME);
		// TODO Auto-generated method stub
		 CollectorRegistry registry = CollectorRegistry.defaultRegistry;
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
					 
					 
					 //TODO remove test
					 if(metricName.equals("_storm___complete_latency_default")){
						 this.sendEmail("trovata metrica "+metricName+" "+innerValue, "TROVATA", "paride.casulli@gmail.com");
					 }
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
					Gauge duration	=	null;
					if(this.gauges.containsKey(metricName)){
						duration	=	this.gauges.get(metricName);
					}
					else{
						 duration = Gauge.build()
							     .name(metricName)
							     .help(metricName)
							     .labelNames(labelNames)
							     .register(registry);
						 this.gauges.put(metricName, duration);
					}
					
					
					 Gauge.Child gaugeChild	=	new Gauge.Child();
					 gaugeChild.set(gaugeValue);
					 duration.setChild(gaugeChild, topFeat);				 
					 LOG.info("SONDA-INSIDE-INSIDE checkup "+gaugeValue+" "+duration.toString()+" "+registry.toString());
						
				 }
			 }
			 String metricName	=	dp.name;
			 metricName	=	metricName.replace('-', '_');
			 metricName	=	metricName.replace('/', '_');
			 if(metricName.equals("_storm___complete_latency_default")){
				 this.sendEmail("trovata metrica "+metricName+" "+dp.value, "TROVATA", "paride.casulli@gmail.com");
			 }
			 if(dp.value instanceof Double){
					String[] topFeat	=	new String[1];
					topFeat[0]	=	topName+"";
					String[] labelNames	=	new String[1];
					labelNames[0]			=	"topology";
					Gauge duration	=	null;
					if(this.gauges.containsKey(metricName)){
						duration	=	this.gauges.get(metricName);
					}
					else{
						 duration = Gauge.build()
							     .name(metricName)
							     .help(metricName)
							     .labelNames(labelNames)
							     .register(registry);
						 this.gauges.put(metricName, duration);
					}
					
				 
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
					Gauge duration	=	null;
					if(this.gauges.containsKey(metricName)){
						duration	=	this.gauges.get(metricName);
					}
					else{
						 duration = Gauge.build()
							     .name(metricName)
							     .help(metricName)
							     .labelNames(labelNames)
							     .register(registry);
						 this.gauges.put(metricName, duration);
					}
					
				 Gauge.Child gaugeChild	=	new Gauge.Child();
				 gaugeChild.set(((Long)dp.value).doubleValue());
				 duration.setChild(gaugeChild, topFeat);
			 }
			 else if(dp.value instanceof Integer){
					String[] topFeat	=	new String[1];
					topFeat[0]	=	topName+"";
					String[] labelNames	=	new String[1];
					labelNames[0]			=	"topology";
					Gauge duration	=	null;
					if(this.gauges.containsKey(metricName)){
						duration	=	this.gauges.get(metricName);
					}
					else{
						 duration = Gauge.build()
							     .name(metricName)
							     .help(metricName)
							     .labelNames(labelNames)
							     .register(registry);
						 this.gauges.put(metricName, duration);
					}
					
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
