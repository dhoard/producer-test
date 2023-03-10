package com.github.dhoard.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.FileReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

public class ProducerTest {

    private static final Random RANDOM = new Random();

    private String topic;
    private int recordSize = 100;
    private int recordCount = 1000;
    private int batchSize = 1;

    public static void main(String[] args) throws Exception {
        new ProducerTest().run(args);
    }

    public void run(String[] args) throws Exception {
        if ((args == null) || (args.length != 1)) {
            System.out.println("Usage: java -jar <jar> <test properties>");
            return;
        }

        Properties properties = new Properties();

        try (Reader reader = new FileReader(args[0])) {
            properties.load(reader);
        }

        topic = properties.getProperty("topic");
        recordSize = Integer.parseInt(properties.getProperty("record.size"));
        recordCount = Integer.parseInt(properties.getProperty("record.count"));
        batchSize = Integer.parseInt(properties.getProperty("batch.size"));

        KafkaProducer<String, byte[]> kafkaProducer = null;

        try {
            Properties producerProperties = (Properties) properties.clone();
            producerProperties.remove("topic");
            producerProperties.remove("record.size");
            producerProperties.remove("record.count");

            Timer timer = new Timer();
            kafkaProducer = new KafkaProducer<>(producerProperties);

            for (int i = 0; i < recordCount; i++) {
                byte[] bytes = randomString(recordSize).getBytes(StandardCharsets.UTF_8);

                ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, null, bytes);
                ExtendedCallback extendedCallback = new ExtendedCallback(producerRecord, timer);

                timer.start();

                Future<RecordMetadata> future = kafkaProducer.send(producerRecord, extendedCallback);
                future.get();

                if (extendedCallback.getException() != null) {
                    System.out.println("Exception");
                    extendedCallback.getException().printStackTrace();
                }

                if (extendedCallback.isError()) {
                    System.out.println("isError() " + extendedCallback.isError());
                }

                if (extendedCallback.getException() == null && !extendedCallback.isError()) {
                    //System.out.println("send passed");
                }
            }

            System.out.println("topic        : " + topic);
            System.out.println("record size  : " + recordSize + " bytes");
            System.out.println("record count : " + recordCount);
            System.out.println("batch size   : " + batchSize);
            System.out.println("time         : " + timer.getTime() + " ms");
            System.out.println("min          : " + timer.getMin() + " ms");
            System.out.println("max          : " + timer.getMax() + " ms");
            System.out.println("median       : " + timer.getMedian() + " ms");
            System.out.println("mean         : " + timer.getMean() + " ms");
            System.out.println("99th %-tile  : " + timer.getPercentile(99) + " ms");
            System.out.println("95th %-tile  : " + timer.getPercentile(95) + " ms");
            System.out.println("90th %-tile  : " + timer.getPercentile(90) + " ms");
            System.out.println("80th %-tile  : " + timer.getPercentile(80) + " ms");
            System.out.println("70th %-tile  : " + timer.getPercentile(70) + " ms");
            System.out.println("60th %-tile  : " + timer.getPercentile(60) + " ms");
            System.out.println("50th %-tile  : " + timer.getPercentile(50) + " ms");
            System.out.println("40th %-tile  : " + timer.getPercentile(40) + " ms");
            System.out.println("30th %-tile  : " + timer.getPercentile(30) + " ms");
            System.out.println("20th %-tile  : " + timer.getPercentile(20) + " ms");
            System.out.println("10th %-tile  : " + timer.getPercentile(10) + " ms");
            System.out.println("rate         : " + ((double) recordCount) / (timer.getTime() / 1000.0d) + " records per second");

            /*
            System.out.println();
            List<Long> timeList = timer.getTimeListUnsorted();
            for (Long time : timeList) {
                System.out.print(time + " ");
            }
            System.out.println();
            System.out.println();
            timeList = timer.getTimeListSorted();
            for (Long time : timeList) {
                System.out.print(time + " ");
            }
            System.out.println();
            */
        } finally {
            if (null != kafkaProducer) {
                kafkaProducer.flush();
                kafkaProducer.close();
            }
        }
    }

    public static class ExtendedCallback implements Callback {

        private ProducerRecord<String, byte[]> producerRecord;

        private RecordMetadata recordMetadata;

        private Exception exception;

        private Timer timer;

        public ExtendedCallback(ProducerRecord<String, byte[]> producerRecord, Timer timer) {
            this.producerRecord = producerRecord;
            this.timer = timer;
        }

        public boolean isError() {
            return (null != this.exception);
        }

        public ProducerRecord<String, byte[]> getProducerRecord() {
            return this.producerRecord;
        }

        public RecordMetadata getRecordMetadata() {
            return this.recordMetadata;
        }

        public Exception getException() {
            return this.exception;
        }

        public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
            this.timer.stop();
            this.recordMetadata = recordMetadata;
            this.exception = exception;
        }
    }

    private String randomString(int length) {
        return RANDOM.ints(48, 122 + 1)
            .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
            .limit(length)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();
    }
}
