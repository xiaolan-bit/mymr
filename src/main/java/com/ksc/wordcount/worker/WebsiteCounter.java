package com.ksc.wordcount.worker;



import com.ksc.wordcount.task.KeyValue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.List;
import java.util.ArrayList;

public class WebsiteCounter {
    public static void main(String[] args) {
        String filename = "D:/tmp/input/allInputFile4.log"; // 替换为你的数据文件路径

        String osName = System.getProperty("os.name");

        if (osName.toLowerCase().contains("windows")) {
            System.out.println("Running on Windows");
        } else if (osName.toLowerCase().contains("linux")) {
            System.out.println("Running on Linux");
        } else {
            System.out.println("Running on " + osName);
        }
        List<KeyValue<String, Integer>> websiteCountList = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            Pattern pattern = Pattern.compile("http[^\"]*");

            websiteCountList = br.lines()
                    .flatMap(line -> {
                        Matcher matcher = pattern.matcher(line);
                        List<KeyValue<String, Integer>> list = new ArrayList<>();
                        while (matcher.find()) {
                            String url = matcher.group();
                            int spaceIndex = url.indexOf(" ");
                            if (spaceIndex != -1) {
                                url = url.substring(0, spaceIndex);
                            }
                            list.add(new KeyValue<>(url, 1));
                        }
                        return list.stream();
                    })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        websiteCountList.forEach(kv -> {
            System.out.println("Website " + kv.getKey() + ": " + kv.getValue() + " times");
        });
    }
}

