package com.ksc.wordcount.conf;

public class AppConfig {

    public static String getShuffleTempDir(String applicationId) {
        String baseDir = System.getProperty("java.io.tmpdir");
        if (baseDir == null || baseDir.isEmpty()) {
            baseDir = "/tmp/shuffle";
        }
        return baseDir + "/" + applicationId;
    }
}
