package org.abate.bigdatademo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

public class BigData {
    public static void main(String[] args)  {
        try {
            var startTime = System.currentTimeMillis();
            long result = Files.lines(Path.of("/Users/MercyBate/Downloads/Hr5m.csv")).parallel()
//
                    .skip(1)
                    .map(s -> s.split(","))
                    .map(arr -> arr[25])
                    .mapToLong(sal ->Long.parseLong(sal))
                    .sum();
//                    .collect(Collectors.summingLong(sal -> Long.parseLong(sal)));
            var endTime = System.currentTimeMillis();
            System.out.printf("$%,d.00%n", result);
            System.out.println(endTime - startTime);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
