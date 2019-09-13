package org.happypants.kafka.testHelpers;

import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.happypants.kafka.testHelpers.test_applications.WordCount;

class WordCountWithStaticTopologyTest {
    private final WordCount app = new WordCount();

    final TestTopology<Object, String> testTopology =
            new TestTopology<>(this.app::getTopology, this.app.getKafkaProperties());

    @BeforeEach
    void start() {
        this.testTopology.start();
    }

    @AfterEach
    void stop() {
        this.testTopology.stop();
    }

    @Test
    void shouldAggregateSameWordStream() {
        this.testTopology.input().add("bla")
            .add("blub")
            .add("bla");

        this.testTopology.streamOutput().withSerde(Serdes.String(), Serdes.Long())
            .expectNextRecord().hasKey("bla").hasValue(1L)
            .expectNextRecord().hasKey("blub").hasValue(1L)
            .expectNextRecord().hasKey("bla").hasValue(2L)
            .expectNoMoreRecord();
    }
}
