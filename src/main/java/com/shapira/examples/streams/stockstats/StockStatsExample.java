package com.shapira.examples.streams.stockstats;

import com.shapira.examples.streams.stockstats.serde.JsonDeserializer;
import com.shapira.examples.streams.stockstats.serde.JsonSerializer;
import com.shapira.examples.streams.stockstats.serde.WrapperSerde;
import com.shapira.examples.streams.stockstats.model.Trade;
import com.shapira.examples.streams.stockstats.model.TradeStats;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Properties;

/**
 * Input is a stream of trades
 * Output is two streams: One with minimum and avg "ASK" price for every 10 seconds window
 * Another with the top-3 stocks with lowest minimum ask every minute
 *
 * @formatter:off
 *
 * Topologies:
 *    Sub-topology: 0
 *     Source: KSTREAM-SOURCE-0000000000 (topics: [stocks])
 *       --> KSTREAM-AGGREGATE-0000000001
 *
 *     Processor: KSTREAM-AGGREGATE-0000000001 (stores: [trade-aggregates])
 *       <-- KSTREAM-SOURCE-0000000000
 *       --> KTABLE-TOSTREAM-0000000002
 *
 *     Processor: KTABLE-TOSTREAM-0000000002 (stores: [])
 *       <-- KSTREAM-AGGREGATE-0000000001
 *       --> KSTREAM-MAPVALUES-0000000003
 *
 *     Processor: KSTREAM-MAPVALUES-0000000003 (stores: [])
 *       <-- KTABLE-TOSTREAM-0000000002
 *       --> KSTREAM-SINK-0000000004
 *
 *     Sink: KSTREAM-SINK-0000000004 (topic: stockstats-output)
 *       <-- KSTREAM-MAPVALUES-0000000003
 *
 * @formatter:on
 *
 * 线程 stockstat-2-aa8b033d-1d47-4190-b402-f537867f9f0d-StreamThread-1
 * kafka consumer poll， 塞入Task Queue
 * 从Task Queue取出head任务， process，
 *
 * TimeWindowedKStream 处理方式，底层用了TimestampedWindowStore， 用了RocksDBWindowStore？， 底层用了Segments，底层用了TreeMap<Long, ? extends Segment>
 *
 */
public class StockStatsExample {

    public static void main(String[] args) throws Exception {

//        Properties props;
//        if (args.length==1)
//            props = LoadConfigs.loadConfig(args[0]);
//        else
//            props = LoadConfigs.loadConfig();

        Properties props = new Properties();
        props.put("bootstrap.servers",Constants.BROKER);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stockstat-2");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TradeSerde.class.getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // creating an AdminClient and checking the number of brokers in the cluster, so I'll know how many replicas we want...

        AdminClient ac = AdminClient.create(props);
        DescribeClusterResult dcr = ac.describeCluster();
        int clusterSize = dcr.nodes().get().size();

        if (clusterSize<3)
            props.put("replication.factor",clusterSize);
        else
            props.put("replication.factor",3);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Trade> source = builder.stream(Constants.STOCK_TOPIC);

        KStream<Windowed<String>, TradeStats> stats = source // KStream
                .groupByKey() // KStream -> KGroupStream
                .windowedBy(TimeWindows.of(Duration.ofMillis(5000)).advanceBy(Duration.ofMillis(1000))) // KGroupStream -> TimeWindowedKStream
                .<TradeStats>aggregate(TradeStats::new, (k, v, tradeStats) -> tradeStats.add(v),
                        Materialized.<String, TradeStats, WindowStore<Bytes, byte[]>>as("trade-aggregates")
                                .withValueSerde(new TradeStatsSerde()))  // TimeWindowedKStream -> KTable
                .toStream() // KTable -> KStream
                .mapValues(TradeStats::computeAvgPrice); // KStream -> KStream

        // 产出是对的，一般都是1s发一个，每个是5s的结果聚合，但有例外，4个，3个结果集合
        // 例外的原因是：producer（stocks topic）先停，然后每个窗口保有的消息数逐步减少
        stats.to("stockstats-output", Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class)));

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        System.out.println("show topology:\n"+ topology.describe());

        streams.cleanUp();

        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    static public final class TradeSerde extends WrapperSerde<Trade> {
        public TradeSerde() {
            super(new JsonSerializer<Trade>(), new JsonDeserializer<Trade>(Trade.class));
        }
    }

    static public final class TradeStatsSerde extends WrapperSerde<TradeStats> {
        public TradeStatsSerde() {
            super(new JsonSerializer<TradeStats>(), new JsonDeserializer<TradeStats>(TradeStats.class));
        }
    }

}
