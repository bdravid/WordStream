package com.bdravid.numberstream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.bdravid.numberstream.NumberStreamPublisher.TOPIC_NUMBER_STREAM;

public class NumberStreamConsumer {
    private static final Logger logger = LoggerFactory.getLogger(NumberStreamConsumer.class);
    private KafkaConsumer<Integer, Integer> consumer;
    private boolean consume;
    private LinkedBlockingQueue<ConsumerRecord<Integer, Integer>> numbersQ = new LinkedBlockingQueue<>();

    private void init(String id, boolean fromBeginning){
        KafkaConsumerBuilder<Integer, Integer> consumerBuilder =
                new KafkaConsumerBuilder<>("localhost:9093", "*",
                        IntegerDeserializer.class, IntegerDeserializer.class);
        this.consumer = consumerBuilder.withClientId(id).build();
        consumer.subscribe(Collections.singleton(TOPIC_NUMBER_STREAM));
        if(fromBeginning){
            consumer.poll(0);
            consumer.seekToBeginning(Collections.emptyList());
        }
        this.consume = true;
    }

    public static void main(String[] args) {
        String id = args[0];
        NumberStreamConsumer consumer = new NumberStreamConsumer();
        consumer.init(id, true);
        Executors.newSingleThreadExecutor().submit(consumer::subscribe);
        Executors.newSingleThreadExecutor().submit(() -> {
            logger.info("Type stop to end");
            Scanner scanner = new Scanner(System.in);
            while (scanner.hasNext()) {
                String word = scanner.next();
                if (word.equalsIgnoreCase("stop")) {
                    consumer.stop();
                    break;
                }
            }
        });
        consumer.printToConsole();
    }

    private void stop() {
        consume = false;
        consumer.wakeup();
        consumer.close();
    }

    private void printToConsole(){
        while(consume){
            try {
                ConsumerRecord<Integer, Integer> polled = numbersQ.poll(5, TimeUnit.SECONDS);
                if (polled != null) {
                    logger.info("Received Key : {}, value : {} ", polled.key(), polled.value());
                }
            } catch (InterruptedException e) {
                //do nothing
            }
        }
    }

    private void subscribe() {
        while(consume) {
            ConsumerRecords<Integer, Integer> records = consumer.poll(Long.MAX_VALUE);
            records.forEach(numbersQ::add);
        }
    }
}
