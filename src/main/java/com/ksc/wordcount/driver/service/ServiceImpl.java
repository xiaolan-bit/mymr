package com.ksc.wordcount.driver.service;

import com.ksc.urltopn.thrift.*;
import com.ksc.urltopn.thrift.UrlTopNAppRequest;
import com.ksc.urltopn.thrift.UrlTopNAppResponse;
import com.ksc.urltopn.thrift.UrlTopNResult;
import com.ksc.urltopn.thrift.UrlTopNService;
import com.ksc.wordcount.driver.UrlTopNDriver;
import org.apache.avro.Schema;
import org.apache.thrift.TException;

import java.io.BufferedReader;
import java.io.File;
//import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

public  class ServiceImpl implements UrlTopNService.Iface {

    @Override
    public UrlTopNAppResponse submitApp(UrlTopNAppRequest urlTopNAppRequest) throws TException {

        System.out.println("submitApp in ServiceImpl");
        UrlTopNDriver.submitApp(urlTopNAppRequest);


        UrlTopNAppResponse urlTopNAppResponse = new UrlTopNAppResponse();
        System.out.println(System.getProperty("inputPath"));
        urlTopNAppResponse.setAppStatus(0);
        urlTopNAppResponse.setApplicationId(urlTopNAppRequest.getApplicationId());

        return urlTopNAppResponse;

    }

    @Override
    public UrlTopNAppResponse getAppStatus(String applicationId) throws TException {
        String status = UrlTopNDriver.getStatus(applicationId);
        int appStatus=0;
        if(status.equals("FINISH"))
            appStatus = 2;
        else if(status.equals("RUNNING"))
            appStatus = 1;

        UrlTopNAppResponse urlTopNAppResponse = new UrlTopNAppResponse();
        urlTopNAppResponse.setAppStatus(appStatus);
        urlTopNAppResponse.setApplicationId(applicationId);
        return urlTopNAppResponse;
    }

    @Override
    public List<UrlTopNResult> getTopNAppResult(String applicationId) throws TException {
        String outputPath = UrlTopNDriver.getOutputPath(applicationId) + "/part_000.avro";
        System.out.println("outputPath: " + outputPath);

        List<UrlTopNResult> urlTopNResultList = new ArrayList<>();

        try (DataFileReader<GenericRecord> reader = new DataFileReader<>(new File(outputPath), new GenericDatumReader<>())) {
            Schema schema = reader.getSchema();

            for (GenericRecord record : reader) {
                String url = record.get("key").toString();
                int count = (int) record.get("value");

                UrlTopNResult urlTopNResult = new UrlTopNResult();
                urlTopNResult.setUrl(url);
                urlTopNResult.setCount(count);

                urlTopNResultList.add(urlTopNResult);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return urlTopNResultList;
    }


//    @Override
//    public List<UrlTopNResult> getTopNAppResult(String applicationId) throws TException {
//        //
//        String outputPath = UrlTopNDriver.getOutputPath(applicationId)+"/part_000.txt";
//        System.out.println("outputPath: " + outputPath);
//        //读取outputPath路径的文件
//        outputPath = Paths.get(outputPath).toString();
//        List<UrlTopNResult> urlTopNResultList = new ArrayList<UrlTopNResult>();
//        try (BufferedReader reader = new BufferedReader(new FileReader(outputPath))) {
//            String line;
//            while ((line = reader.readLine()) != null) {
//                String[] parts = line.split("\t");
//                if (parts.length >= 2) {
//                    String url = parts[0];
//                    int count = Integer.parseInt(parts[1]);
//                    UrlTopNResult urlTopNResult = new UrlTopNResult();
//                    urlTopNResult.setUrl(url);
//                    urlTopNResult.setCount(count);
//                    urlTopNResultList.add(urlTopNResult);
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        return urlTopNResultList;
//    }
}
