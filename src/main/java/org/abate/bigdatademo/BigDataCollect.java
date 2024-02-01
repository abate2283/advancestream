package org.abate.bigdatademo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

public class BigDataCollect {
    public static void main(String[] args) {
        try {
            var collectCount = Files.lines(Path.of("/Users/MercyBate/Downloads/Hr5m.csv"))
                    .skip(1)
                    .collect(Collectors.counting());
            System.out.println("Number of Records: " + collectCount);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
