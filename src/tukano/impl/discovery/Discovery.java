package tukano.impl.discovery;

import static java.lang.String.format; 

import java.net.DatagramPacket; 
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.URI;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import tukano.api.Short;
import tukano.api.java.Blobs;
import utils.DB;
import utils.Sleep;

/**
 * <p>A class interface to perform service discovery based on periodic 
 * announcements over multicast communication.</p>
 * 
 */

public interface Discovery {

	/**
	 * Used to announce the URI of the given service name.
	 * @param serviceName - the name of the service
	 * @param serviceURI - the uri of the service
	 */
	public void announce(String serviceName, String serviceURI);

	/**
	 * Get discovered URIs for a given service name
	 * @param serviceName - name of the service
	 * @param minReplies - minimum number of requested URIs. Blocks until the number is satisfied.
	 * @return array with the discovered URIs for the given service name.
	 */
	public URI[] knownUrisOf(String serviceName, int minReplies);

	/**
	 * Get the instance of the Discovery service
	 * @return the singleton instance of the Discovery service
	 */
	public static Discovery getInstance() {
		return DiscoveryImpl.getInstance();
	}
}

/**
 * Implementation of the multicast discovery service
 */
class DiscoveryImpl implements Discovery {
	
	private static Logger Log = Logger.getLogger(Discovery.class.getName());

	static final int DISCOVERY_RETRY_TIMEOUT = 5000;
	static final int DISCOVERY_ANNOUNCE_PERIOD = 1000;
	static final InetSocketAddress DISCOVERY_ADDR = new InetSocketAddress("226.226.226.226", 2266);

	// Used separate the two fields that make up a service announcement.
	private static final String DELIMITER = "\t";

	private static final int MAX_DATAGRAM_SIZE = 65536;

	private static Discovery singleton;

	private Map<String, Set<URI>> uris = new ConcurrentHashMap<>();
	private Map<URI, LocalDateTime> lastHeartbeat = new ConcurrentHashMap<>();
	private List<URI> failedServers = new ArrayList<>();
	
	 private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

	synchronized static Discovery getInstance() {
		if (singleton == null) {
			singleton = new DiscoveryImpl();
		}
		return singleton;
	}
		
	private DiscoveryImpl() {
		this.startListener();
		this.startHealthChecker();
		this.startHealthCheckerShorts();
	}
	
    private void startHealthChecker() {
        scheduler.scheduleAtFixedRate(() -> {
        LocalDateTime now = LocalDateTime.now();

            
        lastHeartbeat.forEach((uri, time) -> {
            	if(now.isAfter(time.plusSeconds((DISCOVERY_ANNOUNCE_PERIOD/1000)*2))) {
            		if(uri.getHost().charAt(0) == 'b') {
            			System.out.println("392483"+ uris);
               		 	handleServerDownBlobs(uri);
               		 	lastHeartbeat.remove(uri);
               		 	failedServers.add(uri);
               		 	uris.get("blobs").remove(uri);
               		 	System.out.println("392483"+ uris);
            		}
            		/*
            		else {
               		 	lastHeartbeat.remove(uri);
               		 	failedServers.add(uri);
               		 	//uris.get("shorts").remove(uri);
               		 	System.out.println("Hdehfi1 "+ uris.get("shorts").remove(uri));
               		 System.out.println("Hdehfi2 "+ uris);
            		}
            		*/

            	}
            });
        }, 0, DISCOVERY_ANNOUNCE_PERIOD, TimeUnit.MILLISECONDS);
    }
    
    private void startHealthCheckerShorts() {
        scheduler.scheduleAtFixedRate(() -> {
        LocalDateTime now = LocalDateTime.now();

            
        lastHeartbeat.forEach((uri, time) -> {
            	if(now.isAfter(time.plusSeconds((DISCOVERY_ANNOUNCE_PERIOD/1000)*2))) {
            		if(uri.getHost().charAt(0) == 's') {
            			System.out.println("39248"+ uris);
               		 	lastHeartbeat.remove(uri);
               		 	failedServers.add(uri);
               		 System.out.println(uris.get("shorts").remove(uri));
               		 	System.out.println("39248"+ uris);
               		 System.out.println("39248"+ uri);
            		}

            	}
            });
        }, 0, DISCOVERY_ANNOUNCE_PERIOD, TimeUnit.MILLISECONDS);
    }
    
    private void handleServerDownBlobs(URI serverUri) {
    	System.out.println("Server down2: " + serverUri);
    	//remove this uri from all shorts, should I do it here or 
    	var query = format("SELECT * FROM Short");
    	List<Short> shorts = DB.sql( query, Short.class);

        for (Short shrt : shorts) {
        	String newBlobUrl = "";
        	String[] blobUrls = shrt.getBlobUrl().split("\\|");

        	for(int i = 0; i < blobUrls.length; i++) {
        		
        		if(!blobUrls[i].contains(serverUri.toString())) {
        			newBlobUrl = newBlobUrl + blobUrls[i] + "|";
        		}

        		
        	}
        	
        	shrt.setBlobUrl(newBlobUrl);
        	DB.updateOne(shrt);
      
        }
        
    	List<Short> shorts1 = DB.sql( query, Short.class);
    	System.out.println("updatedd" + shorts1);
    }

	@Override
	public void announce(String serviceName, String serviceURI) {
		Log.info(String.format("Starting Discovery announcements on: %s for: %s -> %s\n", DISCOVERY_ADDR, serviceName, serviceURI));

		var pktBytes = String.format("%s%s%s", serviceName, DELIMITER, serviceURI).getBytes();
		var pkt = new DatagramPacket(pktBytes, pktBytes.length, DISCOVERY_ADDR);

		// start thread to send periodic announcements
		new Thread(() -> {
			try (var ds = new DatagramSocket()) {
				while (true) {
					try {
						ds.send(pkt);
						Sleep.ms(DISCOVERY_ANNOUNCE_PERIOD);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}).start();
	}


	@Override
	public URI[] knownUrisOf(String serviceName, int minEntries) {
		while(true) {
			var res = uris.getOrDefault(serviceName, Collections.emptySet());
			if( res.size() >= minEntries )
				return res.toArray( new URI[res.size()]);
			else
				Sleep.ms(DISCOVERY_ANNOUNCE_PERIOD);
				
		}
	}

	private void startListener() {
		Log.info(String.format("Starting discovery on multicast group: %s, port: %d\n", DISCOVERY_ADDR.getAddress(), DISCOVERY_ADDR.getPort()));

		new Thread(() -> {
			try (var ms = new MulticastSocket(DISCOVERY_ADDR.getPort())) {
				ms.joinGroup(DISCOVERY_ADDR, NetworkInterface.getByInetAddress(InetAddress.getLocalHost()));
				for (;;) {
					try {
						var pkt = new DatagramPacket(new byte[MAX_DATAGRAM_SIZE], MAX_DATAGRAM_SIZE);
						ms.receive(pkt);
						var msg = new String(pkt.getData(), 0, pkt.getLength());
						Log.finest(String.format("Received: %s", msg));

						var parts = msg.split(DELIMITER);
						if (parts.length == 2) {
							var serviceName = parts[0];
							var uri = URI.create(parts[1]);
							uris.computeIfAbsent(serviceName, (k) -> ConcurrentHashMap.newKeySet()).add( uri );
							
							if(!failedServers.contains(uri)) {
								lastHeartbeat.put(uri, LocalDateTime.now());
							}
									
						}

					} catch (Exception x) {
						x.printStackTrace();
					}
				}
			} catch (Exception x) {
				x.printStackTrace();
			}
		}).start();
	}
}