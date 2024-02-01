package org.abate.bigdatadomainmodel;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class BigDataGroupingMappingToLong {

        record Person(String firstName, String lastName, Long salary, String state){}
    public static void main(String[] args) {
        try {
            long start = System.currentTimeMillis();
            var result = Files.lines(Path.of("/Users/MercyBate/Downloads/Hr5m.csv")).parallel()
                    .skip(1)
//                    .limit(10)
                    .map(x -> x.split(","))
                    .map(x -> new Person(x[2], x[4], Long.parseLong(x[25]), x[32]))
                    .collect(Collectors.groupingBy(Person::state, TreeMap::new, Collectors.summingLong(Person::salary)));
            long end = System.currentTimeMillis();
//            System.out.printf("$%,d.00%n",result);
            System.out.println(result);
            System.out.println(end - start);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
