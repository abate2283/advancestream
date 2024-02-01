package org.abate.bigdatademo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

public class BigDataPractice {
    public static void main(String[] args) {
        try {
            var start = System.currentTimeMillis();
            var totalSalaries = Files.lines(Path.of("/Users/MercyBate/Downloads/Hr5m.csv"))
                    .skip(1)
                    .map(s -> s.split(","))
                    .map(arr -> arr[25])
                    .collect(Collectors.summingLong(sal -> Long.parseLong(sal)));
            var end = System.currentTimeMillis();
            System.out.printf("$%,d.00%n", totalSalaries);
            System.out.println(end - start);


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
