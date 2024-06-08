package tukano.impl.rest.servers;

import java.net.URI;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import tukano.impl.api.java.ExtendedShorts;
import tukano.impl.discovery.Discovery;
import tukano.impl.java.servers.JavaRepShorts;
import tukano.impl.kafka.KafkaPublisher;
import tukano.impl.kafka.KafkaSubscriber;
import tukano.impl.kafka.SyncPoint;
import tukano.impl.kafka.KafkaUtils;
import tukano.impl.kafka.RecordProcessor;
import tukano.impl.rest.clients.RestShortsClient;
import tukano.api.Short;
import tukano.api.java.Result;
import tukano.api.java.Shorts;

import com.google.gson.Gson;



public class ShortsEventService implements RecordProcessor{
    private KafkaPublisher publisher;
    private KafkaSubscriber subscriber;
    
    static final String FROM_BEGINNING = "earliest";
    static final String TOPIC = "shorts-events";
    
    private final String CREATE_SHORT = "createShort";
    private final String GET_SHORT = "getShort";
    private final String DELETE_SHORT = "deleteShort";
    private final String LIKE_SHORT = "likeShort";
    private final String FOLLOW_SHORT = "followShort";
    private final String DELETE_ALL_SHORTS = "deleteAllShort";
    
    final JavaRepShorts impl = new JavaRepShorts();
    
	Gson gson = new Gson();
	
    private Long version;
    
    private SyncPoint<Result<?>> syncPoint;
    

    public ShortsEventService(String kafkaBrokers) {
        this.publisher = KafkaPublisher.createPublisher(kafkaBrokers);
        this.subscriber = KafkaSubscriber.createSubscriber(kafkaBrokers, Arrays.asList(TOPIC), FROM_BEGINNING);
        this.subscriber.start(false, this);
        this.syncPoint = SyncPoint.getInstance();
        //não tenho a certeza se isto é aqui
        
      
    }
    
    long getCurrentVersion() {
    	return version;
    }

    public void onReceive(ConsumerRecord<String, String> r) {
        long offset = r.offset();
        String params = r.value();
        Result<?> res = null;
        
        
        syncPoint.waitForVersion(offset - 1, 500);
        
        
        
        version = offset;
        
        
        switch (r.key()){
            case CREATE_SHORT ->{
            	syncPoint.waitForResult(offset - 1);
                Short newShort = gson.fromJson(params, Short.class);
                
                String shortId = newShort.getOwnerId() + "-"+ offset;
                res = impl.createShort(shortId, newShort.getOwnerId(), newShort.getTimestamp());
            }
            
            case GET_SHORT ->{
                String shortId = params;
                
                res = impl.getShort(shortId);
            }
            case DELETE_SHORT ->{
                String shortId = params;
                
                res = impl.deleteShort(shortId);
            }
            case LIKE_SHORT ->{

            	String[] param = params.split(":");

            	boolean isLiked;
            	if (param[1].equals("1")){
            		isLiked = true;
            	}
            	else {
            		isLiked = false;
            	}

            	res = impl.like(param[0], param[2], isLiked, param[3] );
                
            }
            case FOLLOW_SHORT ->{
            	String[] param = params.split(":");

            	boolean isLiked;
            	if (param[2].equals("1")){
            		isLiked = true;
            	}
            	else {
            		isLiked = false;
            	}
            	
            	res = impl.follow(param[0], param[1], isLiked, param[3] );
            }
            case DELETE_ALL_SHORTS -> {
            	String[] param = params.split(":");
            	System.out.println("hfgj "+ param[0]+param[1]+param[2]);
            	
            	res = impl.deleteAllShorts(param[0],param[1],param[2]);
            	
            }
            
        }
        syncPoint.setResult(offset, res); 
    }

    public<T> Result<T>  publish(Long version, String operation, String value){

		if(this.version != null){
			syncPoint.waitForResult(this.version -1);
        }
		
        long offset = publisher.publish(TOPIC, operation, value);
        //this.version = offset;
		//if(version != null && version < this.version){
            //syncPoint.waitForVersion(version,500);
        //}

        return (Result<T>) syncPoint.waitForResult(offset);
    }
    
   
  
    
    
    
    
    

}
