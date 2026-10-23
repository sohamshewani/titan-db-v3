package com.benchmark.titan;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class TitanMicrobenchmark {

    private static final int WARMUP_ITERATIONS = 10000;
    private static final int MEASUREMENT_ITERATIONS = 50000;

    public static void main(String[] args) {
        BaseConfiguration conf = new BaseConfiguration();
        conf.set("storage.backend", "inmemory");
        
        System.out.println("Initializing TitanDB (In-Memory Backend)...");
        TitanGraph graph = TitanFactory.open(conf);

        try {
            System.out.println("Running warmup phase (" + WARMUP_ITERATIONS + " ops)...");
            runWorkload(graph, WARMUP_ITERATIONS, false);

            System.out.println("Running measurement phase (" + MEASUREMENT_ITERATIONS + " ops)...");
            long[] latencies = new long[MEASUREMENT_ITERATIONS];
            long startTime = System.nanoTime();
            
            runMeasuredWorkload(graph, MEASUREMENT_ITERATIONS, latencies);
            
            long totalTimeNs = System.nanoTime() - startTime;
            double totalSeconds = totalTimeNs / 1_000_000_000.0;
            double opsPerSec = MEASUREMENT_ITERATIONS / totalSeconds;

            Arrays.sort(latencies);
            long p50 = latencies[(int) (MEASUREMENT_ITERATIONS * 0.50)];
            long p99 = latencies[(int) (MEASUREMENT_ITERATIONS * 0.99)];
            long p999 = latencies[(int) (MEASUREMENT_ITERATIONS * 0.999)];

            printResults(opsPerSec, totalSeconds, MEASUREMENT_ITERATIONS, p50, p99, p999);

        } finally {
            if (graph != null && graph.isOpen()) {
                graph.close();
                System.out.println("TitanDB instance closed.");
            }
        }
    }

    private static void runWorkload(TitanGraph graph, int iterations, boolean commitPerOp) {
        for (int i = 0; i < iterations; i++) {
            Vertex v1 = graph.addVertex("id", i);
            Vertex v2 = graph.addVertex("id", i + 1000000);
            v1.addEdge("knows", v2, "weight", 0.5);
            if (commitPerOp) {
                graph.tx().commit();
            }
        }
        graph.tx().commit();
    }

    private static void runMeasuredWorkload(TitanGraph graph, int iterations, long[] latencies) {
        for (int i = 0; i < iterations; i++) {
            long opStart = System.nanoTime();
            
            Vertex v1 = graph.addVertex("benchId", i);
            Vertex v2 = graph.addVertex("benchId", i + 5000000);
            Edge e = v1.addEdge("benchEdge", v2, "metric", i);
            graph.tx().commit();

            long opEnd = System.nanoTime();
            latencies[i] = opEnd - opStart;
        }
    }

    private static void printResults(double opsPerSec, double totalSeconds, int iterations, long p50, long p99, long p999) {
        System.out.println("\n==================================================");
        System.out.println("            TITANDB MICROBENCHMARK RESULTS        ");
        System.out.println("==================================================");
        System.out.printf("%-25s | %-20s\n", "Metric", "Value");
        System.out.println("--------------------------------------------------");
        System.out.printf("%-25s | %-20d\n", "Total Operations", iterations);
        System.out.printf("%-25s | %-20.2f s\n", "Duration", totalSeconds);
        System.out.printf("%-25s | %-20.2f\n", "Throughput (ops/sec)", opsPerSec);
        System.out.printf("%-25s | %-20.2f ms\n", "Latency P50", p50 / 1_000_000.0);
        System.out.printf("%-25s | %-20.2f ms\n", "Latency P99", p99 / 1_000_000.0);
        System.out.printf("%-25s | %-20.2f ms\n", "Latency P99.9", p999 / 1_000_000.0);
        System.out.println("==================================================");
    }
}