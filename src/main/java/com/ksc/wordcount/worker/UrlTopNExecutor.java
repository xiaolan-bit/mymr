package com.ksc.wordcount.worker;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.ksc.wordcount.rpc.Executor.ExecutorActor;
import com.ksc.wordcount.rpc.Executor.ExecutorRpc;
import com.ksc.wordcount.rpc.Executor.ExecutorSystem;
import com.ksc.wordcount.rpc.ExecutorRegister;
import com.ksc.wordcount.shuffle.nettyimpl.server.ShuffleService;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class UrlTopNExecutor {
    public static void main(String[] args) throws InterruptedException {
        List<String[]> slaveInfoList = getSlaveInfoFromConfig();
        String driverIpPort = getDriverIpPortFromConfig();

        List<String> localIpAddresses = getLocalIpAddresses();
        //将127.0.0.1加入到localIpAddresses中
        localIpAddresses.add("127.0.0.1");
        System.out.println("localIpAddresses:" + localIpAddresses);

// Use an enhanced for-each loop to iterate through slaveInfoList and remove entries
// where slaveInfo[0] matches local IP addresses
        List<String[]> filteredSlaveInfoList = new ArrayList<>();
        for (String[] slaveInfo : slaveInfoList) {
            if (localIpAddresses.contains(slaveInfo[0])) {
                filteredSlaveInfoList.add(slaveInfo);
            }
        }

        for (int i = 0; i < filteredSlaveInfoList.size(); i++) {
            String[] slaveInfo = filteredSlaveInfoList.get(i);
            System.out.println("slaveInfo.length:" + slaveInfoList.size());

            ExecutorEnv.host = slaveInfo[0];
            System.out.println("host1111:" + ExecutorEnv.host);

            ExecutorEnv.port = Integer.parseInt(slaveInfo[1]);
            ExecutorEnv.memory = slaveInfo[3];
            ExecutorEnv.driverUrl = "akka.tcp://DriverSystem@" + driverIpPort + "/user/driverActor";
            ExecutorEnv.core = Integer.parseInt(slaveInfo[4]);
            ExecutorEnv.executorUrl = "akka.tcp://ExecutorSystem@" + ExecutorEnv.host + ":" + ExecutorEnv.port + "/user/executorActor"+i;
            ExecutorEnv.shufflePort = Integer.parseInt(slaveInfo[2]);

            new Thread(() -> {
                try {
                    new ShuffleService(ExecutorEnv.shufflePort).start();
                } catch (InterruptedException e) {
                    new RuntimeException(e);
                }
            }).start();

            ExecutorSystem.setExecutorSystem(null);
            ActorSystem executorSystem = ExecutorSystem.getExecutorSystem();
            ActorRef clientActorRef = executorSystem.actorOf(Props.create(ExecutorActor.class), "executorActor"+i);
            System.out.println("ServerActor started at: " + clientActorRef.path().toString());
            ExecutorRpc.register(new ExecutorRegister(ExecutorEnv.executorUrl, ExecutorEnv.memory, ExecutorEnv.core));
        }



    }

    //通过读取/bin目录下的master.conf的除注释外的前两个以空格拆分的，并组成一个字符串返回
    public static String getDriverIpPortFromConfig(){
        Properties properties = new Properties();
//        String[][] slaveInfo = new String[2][5];
        String[] parts = new String[4];
        String osName = System.getProperty("os.name");
        String confPath = "master.conf";
        if (osName.toLowerCase().contains("windows")) {
            confPath = "bin/master.conf";
        } else if (osName.toLowerCase().contains("linux")) {
            confPath = "master.conf";
        } else {
            System.out.println("Running on " + osName);
        }
        try {
            BufferedReader reader = new BufferedReader(new FileReader(confPath));
            String line;
            int i = 0;
            while ((line = reader.readLine()) != null) {
                // Skip comment lines
                if (line.trim().startsWith("#") || line.trim().isEmpty()) {
                    continue;
                }

                // Split line by whitespace and get key-value pairs
                parts = line.split("\\s+");
                if (parts.length >= 4) {
                    String ipAddress = parts[0];
                    int akkaPort = Integer.parseInt(parts[1]);
                }
                System.out.println(parts[0] + " " + parts[1]);
                i++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return parts[0]+":"+parts[1];
    }
    public static List<String[]> getSlaveInfoFromConfig() {
        List<String[]> slaveInfoList = new ArrayList<>();
        String osName = System.getProperty("os.name");
        String confPath = "slave.conf";
        if (osName.toLowerCase().contains("windows")) {
            confPath = "bin/slave.conf";
        } else if (osName.toLowerCase().contains("linux")) {
            confPath = "slave.conf";
        } else {
            System.out.println("Running on " + osName);
        }
        try {
            BufferedReader reader = new BufferedReader(new FileReader(confPath));
            String line;

            while ((line = reader.readLine()) != null) {
                // Skip comment lines and empty lines
                if (line.trim().startsWith("#") || line.trim().isEmpty()) {
                    continue;
                }

                // Split line by whitespace and get key-value pairs
                String[] parts = line.split("\\s+");
                slaveInfoList.add(parts);
                System.out.println(parts[0] + " " + parts[1] + " " + parts[2] + " " + parts[3] + " " + parts[4]);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return slaveInfoList;
    }

    public static String startUrlTopNExecutor(String[] slaveInfo, String driverIpPort){

        ExecutorEnv.host=slaveInfo[0];
        ExecutorEnv.port= Integer.parseInt(slaveInfo[1]);
        ExecutorEnv.memory=slaveInfo[3];
        ExecutorEnv.driverUrl="akka.tcp://DriverSystem@"+driverIpPort+"/user/driverActor";
        ExecutorEnv.core= Integer.parseInt(slaveInfo[4]);
        ExecutorEnv.executorUrl="akka.tcp://ExecutorSystem@"+ ExecutorEnv.host+":"+ExecutorEnv.port+"/user/executorActor";
        ExecutorEnv.shufflePort=7337;

        new Thread(() -> {
            try {
                new ShuffleService(ExecutorEnv.shufflePort).start();
            } catch (InterruptedException e) {
                new RuntimeException(e);
            }
        }).start();

        ActorSystem executorSystem = ExecutorSystem.getExecutorSystem();
        ActorRef clientActorRef = executorSystem.actorOf(Props.create(ExecutorActor.class), "executorActor");
        System.out.println("ServerActor started at: " + clientActorRef.path().toString());
        ExecutorRpc.register(new ExecutorRegister(ExecutorEnv.executorUrl,ExecutorEnv.memory,ExecutorEnv.core));

        return "success";
    }

    public static List<String> getLocalIpAddresses() {
        List<String> ipAddresses = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();
                Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    InetAddress inetAddress = inetAddresses.nextElement();
                    if (!inetAddress.isLoopbackAddress() && inetAddress.isSiteLocalAddress()) {
                        ipAddresses.add(inetAddress.getHostAddress());
                    }
                }
            }
        } catch (SocketException e) {
            e.printStackTrace();
        }
        return ipAddresses;
    }
}
