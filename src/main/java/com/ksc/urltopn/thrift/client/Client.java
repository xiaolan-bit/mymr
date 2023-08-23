package com.ksc.urltopn.thrift.client;

import com.ksc.urltopn.thrift.*;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;


public class Client {
    public static void main(String[] args) throws Exception {
        // 初始化Thrift，使用TSocket 进行TCP传输，设置连接到 127.0.0.1:9091
        String osName = System.getProperty("os.name");
        TTransport transport = new TSocket("127.0.0.1", 5151);
        transport.open();
        // 使用TBinaryProtocol 序列化协议
        TProtocol protocol = new TBinaryProtocol(transport);
        UrlTopNService.Client client = new UrlTopNService.Client(protocol);
        String confPath = "urltopn.conf";
        if (osName.toLowerCase().contains("windows")) {
            confPath = "bin/urltopn.conf";
        } else if (osName.toLowerCase().contains("linux")) {
            confPath = "urltopn.conf";
        } else {
            System.out.println("Running on " + osName);
        }

        UrlTopNAppRequest urlTopNAppRequest = new UrlTopNAppRequest();
        Properties properties = new Properties();
        String[] parts = new String[6];
        try {

            BufferedReader reader = new BufferedReader(new FileReader(confPath));
            String line;
            while ((line = reader.readLine()) != null) {
                // Skip comment lines
                if (line.trim().startsWith("#") || line.trim().isEmpty()) {
                    continue;
                }
                parts = line.split("\\s+");
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(parts[0] + " " + parts[1] + " " + parts[2] + " " + parts[3] + " " + parts[4] + " " + parts[5]);
        // applicatinId    输入目录          输出目录            topN    过程reduceTask数    分片大小(byte)
        urlTopNAppRequest.setApplicationId("application_test2");
        urlTopNAppRequest.setInputPath(parts[1]);

        //查看系统类型

        if (osName.toLowerCase().contains("windows")) {
            urlTopNAppRequest.setOuputPath("D:/tmp/output_test");
        } else if (osName.toLowerCase().contains("linux")) {
            urlTopNAppRequest.setOuputPath("/root/week5/output_test");
        } else {
            System.out.println("Running on " + osName);
        }
//        urlTopNAppRequest.setOuputPath("D:/tmp/output_test");
        urlTopNAppRequest.setTopN(3);
        urlTopNAppRequest.setNumReduceTasks(Integer.parseInt(parts[4]));
        urlTopNAppRequest.setSplitSize(Integer.parseInt(parts[5]));

        UrlTopNAppResponse urlTopNAppResponse = new UrlTopNAppResponse();

        System.out.println(client.getTopNAppResult("application_1234"));

        urlTopNAppResponse = client.submitApp(urlTopNAppRequest);
        System.out.println(urlTopNAppResponse.toString());

        urlTopNAppResponse = client.getAppStatus(urlTopNAppRequest.getApplicationId());
        System.out.println(urlTopNAppResponse.toString());
        Thread.sleep(50000);
        urlTopNAppResponse = client.getAppStatus(urlTopNAppRequest.getApplicationId());
        System.out.println(urlTopNAppResponse.toString());

        System.out.println(client.getTopNAppResult(urlTopNAppRequest.getApplicationId()).toString());
    }
}