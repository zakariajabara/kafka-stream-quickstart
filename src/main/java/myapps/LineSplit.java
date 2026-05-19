package myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class LineSplit {
    public static void main(String[] args) throws Exception{

        /**
        D abord in specifie differentes parameters de configuration pour l execution
        de Stream comme definit sur StreamsConfig.
        2 des plus importantes valeurs qu on aura besoin de definir sont:
         StreamsConfig.BOOTSTRAP_SERVERS_CONFIG: definit eine liste host/Port, pour l initiale connection
                       au kafka Cluster
         StreamsConfig.APPLICATION_ID_CONFIG: ein unique identifiant pour votre Stream application en le
                       distinguant parmit d autres application appelant le meme Cluster
         */
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"streams-linesplit");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");

        /**
         * en plus on peut adapter (Customizing) les defauts valures de Serialization et
         * deserialisation librairies pour les record key/Value
         */
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream("streams-plaintext-input");
        //KStream<String, String> words = source.flatMapValues(value -> Arrays.asList(value.split("\\W+"))).to("streams-linesplit-output");
          source.flatMapValues(value -> Arrays.asList(value.split("\\W+"))).to("streams-linesplit-output");source.to("streams-pipe-output");

        Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook"){
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try{
            streams.start();
            latch.await();
        }catch (Throwable e){
            System.exit(0);
        }
    }
}
