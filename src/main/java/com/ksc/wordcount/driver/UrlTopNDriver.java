package com.ksc.wordcount.driver;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.ksc.urltopn.thrift.UrlTopNAppRequest;
import com.ksc.urltopn.thrift.UrlTopNAppResponse;
import com.ksc.urltopn.thrift.UrlTopNResult;
import com.ksc.urltopn.thrift.UrlTopNService;
import com.ksc.wordcount.datasourceapi.FileFormat;
import com.ksc.wordcount.datasourceapi.PartionFile;
import com.ksc.wordcount.datasourceapi.PartionWriter;
import com.ksc.wordcount.datasourceapi.UnsplitFileFormat;
import com.ksc.wordcount.driver.service.ServiceImpl;
import com.ksc.wordcount.rpc.Driver.DriverActor;
import com.ksc.wordcount.rpc.Driver.DriverSystem;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.map.MapFunction;
import com.ksc.wordcount.task.map.MapTaskContext;
import com.ksc.wordcount.task.reduce.ReduceFunction;
import com.ksc.wordcount.task.reduce.ReduceTaskContext;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UrlTopNDriver{
    private static Queue<UrlTopNAppRequest> urlTopNAppRequestQueue = new LinkedList<>();

    private static Map<String, String> statusMap = new HashMap<>(); // Map to store applicationId -> status

    private static Map<String, String> outputPathMap = new HashMap<>();

    public UrlTopNDriver() {
        statusMap = new HashMap<>();
        outputPathMap = new HashMap<>();
    }

    public void addApplication(String applicationId) {
        // Add a new application with RUNNING status
        statusMap.put(applicationId, "RUNNING");
    }

    public void updateStatus(String applicationId, String newStatus) {
        // Update status for a specific application
        if (statusMap.containsKey(applicationId)) {
            statusMap.put(applicationId, newStatus);
        } else {
            System.out.println("Application with ID " + applicationId + " not found.");
        }
    }

    public static String getStatus(String applicationId) {
        // Get status for a specific application
        return statusMap.getOrDefault(applicationId, "NOT_found");
    }



    public void addOutputPath(String applicationId, String outputPath) {
        // Add a new outputPath with applicationId -> outputPath mapping
        outputPathMap.put(applicationId, outputPath);
        System.out.println("addTooutputPathMap: " + applicationId+":"+outputPath);
    }

    public static String getOutputPath(String applicationId) {
        // Get outputPath for a specific application
        return outputPathMap.getOrDefault(applicationId, "NOT_FOUND");
    }

    public static void  main(String[] args) throws InterruptedException {
//        ServiceImpl service = new ServiceImpl();
//        service.submitApp(urlTopNAppRequest);
        String[] parts = getMasterInfoFromConfig();

        //查看系统类型
        String osName = System.getProperty("os.name");
        //记载运行的状态
        UrlTopNDriver container = new UrlTopNDriver();

        Map<String, String> routeMap = new HashMap<>();


        Thread thriftServerThread = new Thread(() -> {
            try {
                UrlTopNService.Processor<ServiceImpl> processor = new UrlTopNService.Processor<>(new ServiceImpl());
                TServerSocket serverSocket = new TServerSocket(Integer.parseInt(parts[2]));
                TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
                TServer server = new TSimpleServer(new TServer.Args(serverSocket).processor(processor).protocolFactory(protocolFactory));
                System.out.println("Server started on port "+parts[2]+"...");
                server.serve();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thriftServerThread.start(); // 启动Thrift服务器线程


        UrlTopNAppRequest urlTopNAppRequest = new UrlTopNAppRequest();
        String[] urlTopNParts = null;
        //初始化读取配置文件
        String confPath = "urltopn.conf";
        if (osName.toLowerCase().contains("windows")) {
            confPath = "bin/urltopn.conf";
        } else if (osName.toLowerCase().contains("linux")) {
            confPath = "urltopn.conf";
        } else {
            System.out.println("Running on " + osName);
        }

        urlTopNParts = getUrlTopNInfoFromConfig(confPath);
        urlTopNAppRequest.setApplicationId(urlTopNParts[0]);
        urlTopNAppRequest.setInputPath(urlTopNParts[1]);
        urlTopNAppRequest.setOuputPath(urlTopNParts[2]);
        urlTopNAppRequest.setTopN(Integer.parseInt(urlTopNParts[3]));
        urlTopNAppRequest.setNumReduceTasks(Integer.parseInt(urlTopNParts[4]));
        urlTopNAppRequest.setSplitSize(Integer.parseInt(urlTopNParts[5]));
        //将配置文件中的信息写入urlTopNAppRequestList
        urlTopNAppRequestQueue.add(urlTopNAppRequest);


        //driver的注册


        DriverEnv.host = parts[0];
        DriverEnv.port = Integer.parseInt(parts[1]);
        ActorSystem executorSystem = DriverSystem.getExecutorSystem();
        ActorRef driverActorRef = executorSystem.actorOf(Props.create(DriverActor.class), "driverActor");
        System.out.println("ServerActor started at: " + driverActorRef.path().toString());

        while (true) {
            while (urlTopNAppRequestQueue.size() < 1) {
                Thread.sleep(1000);
                System.out.println("waiting for urlTopNAppRequest");
            }

            while (urlTopNAppRequestQueue.size() > 0) {

                UrlTopNAppRequest urlTopNAppRequest1 = urlTopNAppRequestQueue.poll();
                //遍历urlTopNAppRequestList，将里面的数据给下面的参数赋值
                String inputPath = urlTopNAppRequest1.getInputPath();
                //String outputPath = urlTopNParts[2];
                String outputPath = null;
                if (osName.toLowerCase().contains("windows")) {
                    outputPath = "D:/tmp/midResult/";
                } else if (osName.toLowerCase().contains("linux")) {
                    outputPath = "/tmp/midResult/";
                } else {
                    System.out.println("Running on " + osName);
                }

                int reduceTaskNum = urlTopNAppRequest1.getNumReduceTasks();
                String applicationId = urlTopNAppRequest1.getApplicationId();
                String outputPath1 = urlTopNAppRequest1.getOuputPath();
                //将这个App的状态变为running
                container.addApplication(applicationId);
                container.updateStatus(applicationId, "RUNNING");

                //把applicationId的路径放进map中
                routeMap.put(applicationId, urlTopNAppRequest1.getOuputPath());

                int topN = urlTopNAppRequest1.getTopN();
                int splitSize = urlTopNAppRequest1.getSplitSize();

                File directory = new File(outputPath);
                if (!directory.exists()) {
                    if (directory.mkdirs()) {
                        System.out.println("Directory created: " + outputPath);
                    } else {
                        System.err.println("Failed to create directory: " + outputPath);
                    }
                }

                FileFormat fileFormat = new UnsplitFileFormat();
                //PartionFile[]  partionFiles = fileFormat.getSplits(inputPath, 1000);
                PartionFile[] partionFiles = fileFormat.getSplits(inputPath, splitSize);
                System.out.println("partionFiles.length:" + partionFiles.length);
                TaskManager taskScheduler = DriverEnv.taskManager;


                System.out.println(111);

                int mapStageId = 0;

                // 添加stageId和任务的映射
                taskScheduler.registerBlockingQueue(mapStageId, new LinkedBlockingQueue());

                for (PartionFile partionFile : partionFiles) {
                    MapFunction wordCountMapFunction = new MapFunction<String, KeyValue>() {
                        @Override
                        public Stream<KeyValue> map(Stream<String> stream) {
                            // 在这里添加网站统计逻辑
                            Pattern pattern = Pattern.compile("http[^\\s\",]*");

                            Map<String, Integer> websiteCountMap = new HashMap<>();

                            // 将整个文本作为一个字符串传递给 matcher
                            String text = stream.collect(Collectors.joining("\n"));
                            Matcher matcher = pattern.matcher(text);
                            while (matcher.find()) {
                                String url = matcher.group();
                                int spaceIndex = url.indexOf(" ");
                                if (spaceIndex != -1) {
                                    url = url.substring(0, spaceIndex);
                                }

                                // 对 URL 进行进一步处理，去除引号、空格等
                                url = url.replace("\"", "").trim();

                                // 跳过空白和错误行
                                if (!url.isEmpty() && !url.equals("ERROR")) {
                                    websiteCountMap.put(url, websiteCountMap.getOrDefault(url, 0) + 1);
                                }
                            }

                            // 将网站匹配逻辑的结果转化为Stream<KeyValue>
                            return websiteCountMap.entrySet().stream()
                                    .map(entry -> new KeyValue(entry.getKey(), entry.getValue()));
                        }



                    };

                    MapTaskContext mapTaskContext = new MapTaskContext(applicationId, "stage_" + mapStageId, taskScheduler.generateTaskId(), partionFile.getPartionId(), partionFile,
                            fileFormat.createReader(), reduceTaskNum, wordCountMapFunction);
                    taskScheduler.addTaskContext(mapStageId, mapTaskContext);
                }

                //提交stageId
                DriverEnv.taskScheduler.submitTask(mapStageId);
                DriverEnv.taskScheduler.waitStageFinish(mapStageId);


                int reduceStageId = 1;
                taskScheduler.registerBlockingQueue(reduceStageId, new LinkedBlockingQueue());
                for (int i = 0; i < reduceTaskNum; i++) {
                    ShuffleBlockId[] stageShuffleIds = taskScheduler.getStageShuffleIdByReduceId(mapStageId, i);
                    ReduceFunction<String, Integer, String, Integer> reduceFunction = new ReduceFunction<String, Integer, String, Integer>() {

                        @Override
                        public Stream<KeyValue<String, Integer>> reduce(Stream<KeyValue<String, Integer>> stream) {
                            HashMap<String, Integer> map = new HashMap<>();
                            //todo done 学生实现 定义reducetask处理数据的规则
                            stream.forEach(e -> {
                                String key = e.getKey();
                                Integer value = e.getValue();
                                if (map.containsKey(key)) {
                                    map.put(key, map.get(key) + value);
                                } else {
                                    map.put(key, value);
                                }
                            });
                            return map.entrySet().stream().map(e -> new KeyValue(e.getKey(), e.getValue()));
                        }
                    };
                    PartionWriter partionWriter = fileFormat.createWriter(outputPath, i);
                    ReduceTaskContext reduceTaskContext = new ReduceTaskContext(applicationId, "stage_" + reduceStageId, taskScheduler.generateTaskId(), i, stageShuffleIds, reduceFunction, partionWriter);
                    taskScheduler.addTaskContext(reduceStageId, reduceTaskContext);
                }

                DriverEnv.taskScheduler.submitTask(reduceStageId);
                DriverEnv.taskScheduler.waitStageFinish(reduceStageId);


                //第二次mapreduce


                inputPath = outputPath;
                outputPath = urlTopNAppRequest1.getOuputPath();
                int newreduceTaskNum = 1;

    //        FileFormat fileFormat1 = new UnsplitFileFormat();
    //        PartionFile[] partionFiles1 = fileFormat1.getSplits(inputPath, 1000);

                taskScheduler = DriverEnv.taskManager;
                mapStageId = 0;


                for (int i = 0; i < newreduceTaskNum; i++) {

                    List<ShuffleBlockId[]> shuffleBlockIdList = new ArrayList<>();

                    for (int j = 0; j < reduceTaskNum; j++) {
                        ShuffleBlockId[] stageShuffleIds = taskScheduler.getStageShuffleIdByReduceId(mapStageId, j);
                        shuffleBlockIdList.add(stageShuffleIds);
                    }

                    int totalLength = shuffleBlockIdList.stream().mapToInt(array -> array.length).sum();
                    ShuffleBlockId[] stageShuffleIds = new ShuffleBlockId[totalLength];

                    int index = 0;
                    for (ShuffleBlockId[] ids : shuffleBlockIdList) {
                        for (ShuffleBlockId id : ids) {
                            stageShuffleIds[index++] = id;
                        }
                    }

                    ReduceFunction<String, Integer, String, Integer> reduceFunction = new ReduceFunction<String, Integer, String, Integer>() {
                        @Override
                        public Stream<KeyValue<String, Integer>> reduce(Stream<KeyValue<String, Integer>> stream) {
                            Map<String, Integer> map = new HashMap<>();

                            stream.forEach(e -> {
                                String key = e.getKey();
                                Integer value = e.getValue();
                                if (map.containsKey(key)) {
                                    map.put(key, map.get(key) + value);
                                } else {
                                    map.put(key, value);
                                }
                            });

                            // 对统计结果进行排序，按照值的降序排列
                            // 对统计结果进行排序，按照值的降序排列
                            List<Map.Entry<String, Integer>> sortedEntries = map.entrySet().stream()
                                    .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                                    .collect(Collectors.toList());

                            List<KeyValue<String, Integer>> result = new ArrayList<>();

// 根据 topN 取出前 topN 行数据
                            int rank = 1;
                            for (int i = 0; i < sortedEntries.size(); i++) {
                                if (i > 0 && !sortedEntries.get(i).getValue().equals(sortedEntries.get(i - 1).getValue())) {
                                    rank++;
                                }
                                if (rank <= topN) {
                                    result.add(new KeyValue(sortedEntries.get(i).getKey(), sortedEntries.get(i).getValue()));
                                } else {
                                    break;
                                }
                            }


                            return result.stream().map(entry -> new KeyValue(entry.getKey(), entry.getValue()));
                        }
                    };




                    //根据reduceTask
//                    ReduceFunction<String, Integer, String, Integer> reduceFunction = new ReduceFunction<String, Integer, String, Integer>() {
//                        @Override
//                        public Stream<KeyValue<String, Integer>> reduce(Stream<KeyValue<String, Integer>> stream) {
//                            Map<String, Integer> map = new HashMap<>();
//
//                            stream.forEach(e -> {
//                                String key = e.getKey();
//                                Integer value = e.getValue();
//                                if (map.containsKey(key)) {
//                                    map.put(key, map.get(key) + value);
//                                } else {
//                                    map.put(key, value);
//                                }
//                            });
//
//                            // 对统计结果进行排序，按照值的降序排列
//                            List<Map.Entry<String, Integer>> sortedEntries = map.entrySet().stream()
//                                    .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
//                                    .collect(Collectors.toList());
//
//                            List<KeyValue<String, Integer>> result = new ArrayList<>();
//
//                            // 根据 topN 取出前 topN 行数据
//                            for (int i = 0; i < Math.min(topN, sortedEntries.size()); i++) {
//                                result.add(new KeyValue(sortedEntries.get(i).getKey(), sortedEntries.get(i).getValue()));
//                            }
//
//                            // 如果 topN 大于等于 sortedEntries 的大小，直接返回结果
//                            if (topN >= sortedEntries.size()) {
//                                return result.stream();
//                            }
//
//                            // 读取第 topN 行的 value
//                            Integer topNValue = sortedEntries.get(topN - 1).getValue();
//
//                            // 从 topN+1 行开始往后判断，如果值等于 topN 行的值，则加入结果中
//                            for (int i = topN; i < sortedEntries.size(); i++) {
//                                if (sortedEntries.get(i).getValue().equals(topNValue)) {
//                                    result.add(new KeyValue(sortedEntries.get(i).getKey(), sortedEntries.get(i).getValue()));
//                                } else {
//                                    break; // 结束循环，因为后面的值不等于 topN 行的值
//                                }
//                            }
//
//                            return result.stream().map(entry -> new KeyValue(entry.getKey(), entry.getValue()));
//                        }
//                    };

                    PartionWriter partionWriter = fileFormat.createWriter(outputPath, i);
                    ReduceTaskContext reduceTaskContext = new ReduceTaskContext(applicationId, "stage_" + reduceStageId, taskScheduler.generateTaskId(), i, stageShuffleIds, reduceFunction, partionWriter);
                    taskScheduler.addTaskContext(reduceStageId, reduceTaskContext);


                }

                DriverEnv.taskScheduler.submitTask(reduceStageId);
                DriverEnv.taskScheduler.waitStageFinish(reduceStageId);

                System.out.println("job finished");
                // Update status to FINISH
                container.updateStatus(applicationId, "FINISH");
                //程序结束之后把路径存入map中
                container.addOutputPath(applicationId, outputPath1);
                //程序结束之后清空三个map
                taskScheduler.clearTaskStatusMap();
                taskScheduler.clearStageIdToBlockingQueueMap();
                taskScheduler.clearStageMap();
            }
    }


    }

    public static String[] getMasterInfoFromConfig(){
        Properties properties = new Properties();
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
                    int thriftPort = Integer.parseInt(parts[2]);
                    String memory = parts[3];
                }
                System.out.println(parts[0] + " " + parts[1] + " " + parts[2] + " " + parts[3]);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return parts;
    }

    public static String[] getUrlTopNInfoFromConfig(String fileName){
        Properties properties = new Properties();
        String[] parts = new String[6];
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = reader.readLine()) != null) {
                // Skip comment lines
                if (line.trim().startsWith("#") || line.trim().isEmpty()) {
                    continue;
                }

                // Split line by whitespace and get key-value pairs
                parts = line.split("\\s+");
                System.out.println(parts[0] + " " + parts[1] + " " + parts[2] + " " + parts[3]);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return parts;
    }


    public static UrlTopNAppResponse submitApp(UrlTopNAppRequest urlTopNAppRequest) throws TException {
        System.out.println("submitApp");
        System.out.println(urlTopNAppRequest.getOuputPath());
        urlTopNAppRequestQueue.add(urlTopNAppRequest);
        return null;
    }


    public UrlTopNAppResponse getAppStatus(String applicationId) throws TException {
        return null;
    }


    public List<UrlTopNResult> getTopNAppResult(String applicationId) throws TException {
        return null;
    }
}
