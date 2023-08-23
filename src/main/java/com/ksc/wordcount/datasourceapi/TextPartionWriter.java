package com.ksc.wordcount.datasourceapi;

import com.ksc.wordcount.task.KeyValue;
import org.apache.http.util.ByteArrayBuffer;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.util.stream.Stream;
import java.io.*;
import java.util.stream.Stream;

public class TextPartionWriter implements PartionWriter<KeyValue>, Serializable {

    private String destDest;
    private int partionId;

    public TextPartionWriter(String destDest,int partionId){
        this.destDest = destDest;
        this.partionId = partionId;
    }


    //把partionId 前面补0，补成length位
    public String padLeft(int partionId,int length){
        String partionIdStr = String.valueOf(partionId);
        int len = partionIdStr.length();
        if(len<length){
            for(int i=0;i<length-len;i++){
                partionIdStr = "0"+partionIdStr;
            }
        }
        return partionIdStr;
    }

    //todo done 学生实现 将reducetask的计算结果写入结果文件中
    @Override
    public void write(Stream<KeyValue> stream) throws IOException {
        File file = new File(destDest + File.separator + "part_" + padLeft(partionId, 3) + ".avro");

        String destinationDirectoryPath = new File(destDest + File.separator + "part_" + padLeft(partionId, 3) + ".avro").getParent();
        // Create directory if not exists
        File destinationDirectory = new File(destinationDirectoryPath);
        if (!destinationDirectory.exists()) {
            if (destinationDirectory.mkdirs()) {
                System.out.println("Directory created: " + destinationDirectoryPath);
            } else {
                System.err.println("Failed to create directory: " + destinationDirectoryPath);
            }
        }

        // Create Avro schema
        Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"KeyValue\",\"fields\":[{\"name\":\"key\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"int\"}]}");

        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            writer.create(schema, file);

            stream.forEach(keyValue -> {
                GenericRecord record = new GenericData.Record(schema);
                record.put("key", keyValue.getKey());
                record.put("value", keyValue.getValue());

                try {
                    writer.append(record);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
