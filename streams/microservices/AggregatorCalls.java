package io.confluent.examples.streams.microservices;

import static io.confluent.examples.streams.avro.microservices.Calls.newBuilder;
import static io.confluent.examples.streams.avro.microservices.CallsState.VALIDATED;
import static io.confluent.examples.streams.avro.microservices.CallsValidationResult.FAIL;
import static io.confluent.examples.streams.avro.microservices.CallsValidationResult.PASS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.CallsS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.Calls_VALIDATIONS;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.MIN;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.addShutdownHookAndBlock;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.baseStreamsConfig;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.parseArgsAndConfigure;

import io.confluent.examples.streams.avro.microservices.Calls;
import io.confluent.examples.streams.avro.microservices.CallsState;
import io.confluent.examples.streams.avro.microservices.CallsValidation;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A simple service which listens to to validation results from each of the Validation
 * services and aggregates them by Calls Id, triggering a pass or fail based on whether
 * all rules pass or not.
 */
public class AggregatorCalls implements Service {

  private static final Logger log = LoggerFactory.getLogger(AggregatorCalls.class);
  private final String SERVICE_APP_ID = getClass().getSimpleName();
  private final Consumed<String, CallsValidation> serdes1 = Consumed
      .with(Calls_VALIDATIONS.keySerde(), Calls_VALIDATIONS.valueSerde());
  private final Consumed<String, Calls> serdes2 = Consumed.with(CallsS.keySerde(),
      CallsS.valueSerde());
  private final Serialized<String, CallsValidation> serdes3 = Serialized
      .with(Calls_VALIDATIONS.keySerde(), Calls_VALIDATIONS.valueSerde());
  private final Joined<String, Long, Calls> serdes4 = Joined
      .with(CallsS.keySerde(), Serdes.Long(), CallsS.valueSerde());
  private final Produced<String, Calls> serdes5 = Produced
      .with(CallsS.keySerde(), CallsS.valueSerde());
  private final Serialized<String, Calls> serdes6 = Serialized
      .with(CallsS.keySerde(), CallsS.valueSerde());
  private final Joined<String, CallsValidation, Calls> serdes7 = Joined
      .with(CallsS.keySerde(), Calls_VALIDATIONS.valueSerde(), CallsS.valueSerde());

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

   
    validations
        .groupByKey(serdes3)
        .windowedBy(SessionWindows.with(5 * MIN))
        .aggregate(
            () -> 0L,
            (id, result, total) -> PASS.equals(result.getValidationResult()) ? total + 1 : total,
            (k, a, b) -> b == null ? a : b, //include a merger as we're using session windows.
            Materialized.with(null, Serdes.Long())
        )
        //get rid of window
        .toStream((windowedKey, total) -> windowedKey.key())
        //When elements are evicted from a session window they create delete events. Filter these.
        .filter((k1, v) -> v != null)
        //only include results were all rules passed validation
        .filter((k, total) -> total >= numberOfRules)
        //Join back to Callss
        .join(Callss, (id, Calls) ->
                //Set the Calls to Validated
                newBuilder(Calls).setState(VALIDATED).build()
            , JoinWindows.of(5 * MIN), serdes4)
        //Push the validated Calls into the Callss topic
        .to(CallsS.name(), serdes5);

    //If any rule fails then fail the Calls
    validations.filter((id, rule) -> FAIL.equals(rule.getValidationResult()))
        .join(Callss, (id, Calls) ->
                //Set the Calls to Failed and bump the version on it's ID
                newBuilder(Calls).setState(CallsState.FAILED).build(),
            JoinWindows.of(5 * MIN), serdes7)
        //there could be multiple failed rules for each Calls so collapse to a single Calls
        .groupByKey(serdes6)
        .reduce((Calls, v1) -> Calls)
        //Push the validated Calls into the Callss topic
        .toStream().to(CallsS.name(), Produced.with(CallsS.keySerde(), CallsS.valueSerde()));

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
