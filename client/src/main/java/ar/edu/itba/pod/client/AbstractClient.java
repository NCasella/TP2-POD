package ar.edu.itba.pod.client;

import ar.edu.itba.pod.Cities;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public abstract class AbstractClient {

    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    protected String inPath;
    protected String outPath;
    protected Cities cityParam;

    protected abstract void runClientCode();

    public void ClientMain() throws InterruptedException, IOException, ExecutionException {
        String[] hosts = System.getProperty("serverAddresses").split(";");
        cityParam=Cities.valueOf(System.getProperty("city"));
        inPath=System.getProperty("inPath");
        outPath=System.getProperty("outPath");

        try {
            // Group Config
            GroupConfig groupConfig = new GroupConfig().setName("g7").setPassword("g7pass");

            // Client Network Config
            ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
            clientNetworkConfig.setAddresses(Arrays.stream(hosts).toList());

            // Client Config
            ClientConfig clientConfig = new ClientConfig().setGroupConfig(groupConfig).setNetworkConfig(clientNetworkConfig);

            // Node Client
            HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);
            runClientCode();
        } finally {
            HazelcastClient.shutdownAll();
        }
    }
}


