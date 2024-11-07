package ar.edu.itba.pod.client;

import ar.edu.itba.pod.exceptions.InvalidParamException;
import ar.edu.itba.pod.models.Cities;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public abstract class AbstractClient {

    protected int queryNumber;
    protected String inPath;
    protected String outPath;
    protected Cities cityParam;
    protected List<DistributedObject> distributedCollections;
    protected String idMap = LocalDateTime.now().toString();

    //TODO: hacer el loggeo de los timestamps aca, asi se hace mas generico y menor repeticion de codigo/posibles errores
    protected HazelcastInstance hazelcastInstance;


    protected abstract void runClientCode() throws IOException,ExecutionException,InterruptedException;


    public void clientMain() throws InterruptedException, IOException, ExecutionException {
        try { getParams(); } catch (InvalidParamException e) {
            System.out.println("Error: " + e.getMessage());
            return;
        }
        String[] hosts = System.getProperty("addresses").split(";");
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
            try {
                hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);
            } catch (IllegalStateException e) {
                System.out.println("Error: " + e.getMessage());
                return;
            }
            System.out.println("Starting...");
            try {
                runClientCode();
            } catch (InvalidParamException e) {
                System.out.println("Error: " + e.getMessage());
            } catch (NoSuchFileException e) {
                System.out.println("Error: Invalid Path " + e.getMessage());
            } finally {
                destroyIMaps();
            }

        } finally {
            HazelcastClient.shutdownAll();
        }
    }

    private void destroyIMaps() {
        if (distributedCollections==null) return;
        distributedCollections.forEach(DistributedObject::destroy);
    }

    protected void getParams() {
        if(System.getProperty("addresses")==null)
            throw new InvalidParamException("addresses not specified");

        String cityParamProperty= System.getProperty("city");
        if(cityParamProperty==null)
            throw new InvalidParamException("city parameter not specified");

        Optional<Cities> citiesOptional= Arrays.stream(Cities.values()).filter((cities -> cities.toString().equals(cityParamProperty))).findAny();
        if(citiesOptional.isEmpty())
            throw new InvalidParamException("city parameter not valid");

        cityParam=citiesOptional.get();

        if((inPath=System.getProperty("inPath"))==null)
            throw new InvalidParamException("input path not specified");

        if((outPath=System.getProperty("outPath"))==null)
            throw new InvalidParamException("output path not specified");
    }

    protected int getNParam(int minN ) {
        int nParam;
        try {
            nParam=Integer.parseInt(System.getProperty("n"));
        } catch (NumberFormatException e) {
            throw new InvalidParamException("n parameter is invalid");
        }
        if ( nParam < minN )
            throw new InvalidParamException("n parameter cannot be less than " + minN);
        return nParam;
    }
}


