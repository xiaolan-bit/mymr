package com.ksc.wordcount.shuffle;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.map.MapStatus;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.stream.Stream;

public class DirectShuffleWriter implements ShuffleWriter<KeyValue> {

    String baseDir;
    int reduceTaskNum;
    ObjectOutputStream[] fileWriters;
    ShuffleBlockId[] shuffleBlockIds;
    Kryo kryo; // Kryo instance
    Output[] outputs; // Output instances for each reduce task

    public DirectShuffleWriter(String baseDir, String shuffleId, String applicationId, int mapId, int reduceTaskNum) {
        this.baseDir = baseDir;
        this.reduceTaskNum = reduceTaskNum;
        fileWriters = new ObjectOutputStream[reduceTaskNum];
        shuffleBlockIds = new ShuffleBlockId[reduceTaskNum];
        kryo = new Kryo(); // Create a Kryo instance and register KeyValue.class
        kryo.register(KeyValue.class);

        outputs = new Output[reduceTaskNum]; // Create an array of Output instances

        for (int i = 0; i < reduceTaskNum; i++) {
            try {
                shuffleBlockIds[i] = new ShuffleBlockId(baseDir, applicationId, shuffleId, mapId, i);
                new File(shuffleBlockIds[i].getShuffleParentPath()).mkdirs();
                fileWriters[i] = new ObjectOutputStream(new FileOutputStream(shuffleBlockIds[i].getShufflePath()));

                // Create Output instance for each reduce task
                outputs[i] = new Output(new FileOutputStream(shuffleBlockIds[i].getShufflePath()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void write(Stream<KeyValue> entryStream) throws IOException {
        Iterator<KeyValue> iterator = entryStream.iterator();
        while (iterator.hasNext()) {
            KeyValue keyValue = iterator.next();
            int reduceTaskId = keyValue.getKey().hashCode() % reduceTaskNum;

            // Serialize and write the object using the appropriate Output instance
            kryo.writeObject(outputs[reduceTaskId], keyValue);
        }
        for (Output output : outputs) {
            output.close();
        }
    }

    @Override
    public void commit() {
        for (int i = 0; i < reduceTaskNum; i++) {
            try {
                outputs[i].close(); // Close the Output instance
                fileWriters[i].close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public MapStatus getMapStatus(int mapTaskId) {
        return new MapStatus(mapTaskId, shuffleBlockIds);
    }
}
