package com.example.bigdata.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class WordCountDataUtils {
    public static final List<String> WORD_LIST = Arrays.asList("Spark", "Hadoop", "HBase", "Storm", "Flink", "Hive");

    private static String generateData() {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < 10; i++) {
            Collections.shuffle(WORD_LIST);
            Random random = new Random();
            int endIndex = random.nextInt(WORD_LIST.size()) % (WORD_LIST.size()) + 1;
            String line = StringUtils.join(WORD_LIST.toArray(), "\t", 0, endIndex);
            sb.append(line).append("\n");
        }
        return sb.toString();
    }

    private static void generateDataToHDFS(String hdfsUrl, String user, String outputPathString) {
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI(hdfsUrl), new Configuration(), user);
            Path outputPath = new Path(outputPathString);
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }
            FSDataOutputStream out = fileSystem.create(outputPath);
            out.write(generateData().getBytes());
            out.flush();
            out.close();
            fileSystem.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void generateDataToLocal(String outputPath) {
        try {
            java.nio.file.Path path = Paths.get(outputPath);
            if (Files.exists(path)) {
                Files.delete(path);
            }
            Files.write(path, generateData().getBytes(), StandardOpenOption.CREATE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        generateDataToLocal("./input/input.txt");
//        generateDataToHDFS("hdfs://10.168.1.108:9000",  "root","/wordcount/input.txt");
    }
}
