package org.abate.bigdatadomainmodel;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.reducing;

public class BigDataGroupingPartitionBy {

        record Person(String firstName, String lastName, BigDecimal salary, String state, char gender){}
    public static void main(String[] args) {
        try {
            long start = System.currentTimeMillis();
            var result = Files.lines(Path.of("/Users/MercyBate/Downloads/Hr5m.csv")).parallel()
                    .skip(1)
//                    .limit(10)
                    .map(x -> x.split(","))
                    .map(x -> new Person(x[2], x[4], new BigDecimal(x[25]), x[32], x[5].strip().charAt(0)))
                    .collect(Collectors.groupingBy(Person::state, TreeMap::new, Collectors.groupingBy(Person::gender, collectingAndThen(reducing(BigDecimal.ZERO, Person::salary,(a, b)->a.add(b)),NumberFormat.getCurrencyInstance()::format))));
            long end = System.currentTimeMillis();
//            System.out.printf("$%,d.00%n",result);
            System.out.println(result);
            System.out.println(end - start);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
