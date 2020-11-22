

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.SessionWindows;

/**
 * Aggregation (durationMinutes)  by agentId
 */
public class AggregatorCalls implements Service {

  private static final Logger log = LoggerFactory.getLogger(AggregatorCalls.class);
  private final String SERVICE_APP_ID = getClass().getSimpleName();
  private final Consumed<String, CallsValidation> serdes1 = Consumed
      .with(Calls_VALIDATIONS.keySerde(), Calls_VALIDATIONS.valueSerde());
 
  private KafkaStreams streams;

  @Override
  public void start(final String bootstrapServers, final String stateDir) {
    streams = aggregateCallsValidations(bootstrapServers, stateDir);
    streams.cleanUp(); 
    streams.start();
    log.info("Started Service " + getClass().getSimpleName());
  }

  private KafkaStreams aggregateCallsValidations(
      final String bootstrapServers,
      final String stateDir) {
    final int numberOfRules = 3; 

    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, CallsValidation> validations = builder
        .stream(Calls_VALIDATIONS.name(), serdes1);
    final KStream<String, Calls> Callss = builder
        .stream(CallsS.name(), serdes2)
        .filter((id, Calls) -> CallsState.CREATED.equals(Calls.getState()));

    return new KafkaStreams(builder.build(),
        baseStreamsConfig(bootstrapServers, stateDir, SERVICE_APP_ID));
  }

  @Override
  public void stop() {
    if (streams != null) {
      streams.close();
    }
  }

  public static void main(final String[] args) throws Exception {
    final String bootstrapServers = parseArgsAndConfigure(args);
    final AggregatorCalls service = new AggregatorCalls();
    service.start(bootstrapServers, "/tmp/kafka-streams");
    addShutdownHookAndBlock(service);
  }
}
