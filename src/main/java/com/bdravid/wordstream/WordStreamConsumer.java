package com.bdravid.wordstream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static com.bdravid.wordstream.KafkaConstants.TOPIC_WORD_STREAM;
import static com.bdravid.wordstream.KafkaInteractions.POLL_TIMEOUT_MILLIS;

public class WordStreamConsumer {
    private static Logger logger = LoggerFactory.getLogger(WordStreamConsumer.class);
    private LinkedBlockingQueue<String> messages = new LinkedBlockingQueue<>();
    private boolean consume = true;
    private KafkaConsumer<String, String> consumer;

    public void init(){
        this.consumer = KafkaInteractions.kafkaConsumerSupplier.get();
    }

    public void startConsumption(String topic, boolean fromBeginning) {
        consumer.subscribe(Collections.singleton(topic));
        seekToBeginningIfNeeded(fromBeginning);
        while (consume) {
            ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT_MILLIS);
            for (Iterator<ConsumerRecord<String, String>> it = records.iterator(); it.hasNext(); ) {
                ConsumerRecord<String, String> next = it.next();
                String value = next.value();
                messages.add(value);
            }
        }
        logger.info("Stopping consumer");
    }

    private void seekToBeginningIfNeeded(boolean fromBeginning) {
        if (fromBeginning) {
            consumer.poll(0);
            consumer.seekToBeginning(Collections.singleton(new TopicPartition(TOPIC_WORD_STREAM, 0)));
        }
    }

    public void printMessages() throws InterruptedException {
        while (consume) {
            logger.info(messages.take());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        WordStreamConsumer wordStreamConsumer = new WordStreamConsumer();
        wordStreamConsumer.init();

        try {
            Executors.newFixedThreadPool(1).submit(()->
                    wordStreamConsumer.startConsumption(TOPIC_WORD_STREAM, true));
            wordStreamConsumer.printMessages();
        } finally {
            wordStreamConsumer.close();
        }
    }

    private void close() {
        consume = false;
        consumer.wakeup();
        consumer.close();
        Thread.currentThread().interrupt();
    }
}
