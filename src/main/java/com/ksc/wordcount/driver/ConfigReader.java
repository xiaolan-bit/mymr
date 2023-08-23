package com.ksc.wordcount.driver;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class ConfigReader {
    public static void main(String[] args) {
        Properties properties = new Properties();
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
                // Skip comment lines
                if (line.trim().startsWith("#") || line.trim().isEmpty()) {
                    continue;
                }

                // Split line by whitespace and get key-value pairs
                String[] parts = line.split("\\s+");
                if (parts.length >= 5) {
                    String ipAddress = parts[0];
                    int akkaPort = Integer.parseInt(parts[1]);
                    int rpcPort = Integer.parseInt(parts[2]);
                    String memory = parts[3];
                    int cpu = Integer.parseInt(parts[4]);

                    // Use key-value pairs for your processing
                    // ...
                }
                System.out.println(parts[0] + " " + parts[1] + " " + parts[2] + " " + parts[3] + " " + parts[4]);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
