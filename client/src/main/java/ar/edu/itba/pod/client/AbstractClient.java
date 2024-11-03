package ar.edu.itba.pod.client;

import ar.edu.itba.pod.models.Cities;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public abstract class AbstractClient {

    protected static final Logger logger = LoggerFactory.getLogger(Client.class);
    protected String inPath;
    protected String outPath;
    protected Cities cityParam;


    //TODO: hacer el loggeo de los timestamps aca, asi se hace mas generico y menor repeticion de codigo/posibles errores
    protected HazelcastInstance hazelcastInstance;


    protected abstract void runClientCode() throws IOException,ExecutionException,InterruptedException;

    public void clientMain() throws InterruptedException, IOException, ExecutionException {
        String[] hosts = System.getProperty("addresses").split(";");
        cityParam=Cities.valueOf(System.getProperty("city"));
        inPath=System.getProperty("inPath");
        outPath=System.getProperty("outPath");


        try {
            // Group Config
            GroupConfig groupConfig = new GroupConfig().setName("g7").setPassword("g7pass");

            // Client Network Config
            ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
            clientNetworkConfig.setAddresses(Arrays.stream(hosts).toList());

                // ! ojo, es temporal
                clientNetworkConfig.addAddress("127.0.0.1");

            // Client Config
            ClientConfig clientConfig = new ClientConfig().setGroupConfig(groupConfig).setNetworkConfig(clientNetworkConfig);

            // Node Client
            hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);
            System.out.println("Starting...");
            runClientCode();
        } finally {
            HazelcastClient.shutdownAll();
        }
    }
}


