package ar.edu.itba.pod.client;

import ar.edu.itba.pod.exceptions.InvalidParamException;
import ar.edu.itba.pod.models.Cities;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public abstract class AbstractClient {

    protected int queryNumber;
    protected String inPath;
    protected String outPath;
    protected Cities cityParam;


    //TODO: hacer el loggeo de los timestamps aca, asi se hace mas generico y menor repeticion de codigo/posibles errores
    protected HazelcastInstance hazelcastInstance;


    protected abstract void runClientCode() throws IOException,ExecutionException,InterruptedException;

    public void clientMain() throws InterruptedException, IOException, ExecutionException {
        if(System.getProperty("addresses")==null){
            System.out.println("adresses not specified");
            return;
        }
        String[] hosts = System.getProperty("addresses").split(";");
        String cityParamProperty= System.getProperty("city");
        if(cityParamProperty==null){
            System.out.println("city parameter not specified");
            return;
        }
        Optional<Cities> citiesOptional= Arrays.stream(Cities.values()).filter((cities -> cities.toString().equals(cityParamProperty))).findAny();
        if(citiesOptional.isEmpty()){
            System.out.println("city parameter not valid");
            return;
        }
        cityParam=citiesOptional.get();

        if((inPath=System.getProperty("inPath"))==null){
            System.out.println("input path not specified");
            return;
        }
        if((outPath=System.getProperty("outPath"))==null){
            System.out.println("output path not specified");
            return;
        }
        String logPathString=String.format("%s/time%d.txt",outPath,queryNumber);
        Path logPath= Paths.get(logPathString);
        Files.deleteIfExists(logPath);

        System.setProperty("client.log.file",logPathString);
        try {
            // Group Config
            GroupConfig groupConfig = new GroupConfig().setName("g7").setPassword("g7pass");

            // Client Network Config
            ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
            clientNetworkConfig.setAddresses(Arrays.stream(hosts).toList());

            // Client Config
            ClientConfig clientConfig = new ClientConfig().setGroupConfig(groupConfig).setNetworkConfig(clientNetworkConfig);

            // Node Client
            hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);
            System.out.println("Starting...");
            try {
                runClientCode();
            } catch (InvalidParamException e) {
                System.out.println("Error: " + e.getMessage());
            }

        } finally {
            HazelcastClient.shutdownAll();
        }
    }
}


