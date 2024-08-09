package org.ib;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

public class KafkaProducerStress {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            /* Obtener el tópico del usuario o usar uno predeterminado
            /System.out.print("Ingrese el nombre del tópico (presione Enter para usar el predeterminado): ");
            String topic = scanner.nextLine().trim();
            if (topic.isEmpty()) {
                topic = "topic-prepare-payment-gire-974";
            }
             */
            String topic = "topic-prepare-payment-gire-desa";
            // Obtener la cantidad de mensajes del usuario o usar un valor predeterminado
            System.out.print("Ingrese la cantidad de mensajes (presione Enter para usar el predeterminado): ");
            String messageCountInput = scanner.nextLine().trim();
            int messageCount;
            if (messageCountInput.isEmpty()) {
                messageCount = 10;
            } else {
                messageCount = Integer.parseInt(messageCountInput);
            }

            // Obtener el retraso del usuario o usar un valor predeterminado
            //System.out.print("Ingrese el retraso en milisegundos (presione Enter para usar el predeterminado): ");
            String delayInput = "";//scanner.nextLine().trim();
            int delay;
            if (delayInput.isEmpty()) {
                delay = 0;
            } else {
                delay = Integer.parseInt(delayInput);
            }

            KafkaProducerStress kafkaProducer = new KafkaProducerStress();
            kafkaProducer.produceMessages(topic, messageCount, delay);

            System.out.print("Presione Enter para comenzar una nueva secuencia o escriba 'exit' para salir: ");
            String userInput = scanner.nextLine().trim().toLowerCase();
            if ("exit".equals(userInput)) {
                break;
            }
        }

        scanner.close();
    }

    // Método para enviar mensajes al tópico
    public void produceMessages(String topic, int messageCount, int delay) {
        Producer<String, String> producer = createProducer();
        try {
            for (int i = 0; i < messageCount; i++) {
                String message = "{\n" +
                        "\t\"data\": {\n" +
                        "\t\t\"amount\": 276.5,\n" +
                        "\t\t\"batchId\": \"23dbda37-0d8a-4f96-b9e2-da3a321ba369\",\n" +
                        "\t\t\"businessName\": \"ryzen\",\n" +
                        "\t\t\"communityId\": \"62128\",\n" +
                        "\t\t\"currency\": \"ARS\",\n" +
                        "\t\t\"expirationDate\": \"2024-06-03T15:36:41.795\",\n" +
                        "\t\t\"identifier\": \"mPcIbWoO0d6BeASnT-Lt347IVNErC2Ql4tPA-QeWVBM\",\n" +
                        "\t\t\"ownAccount\": true,\n" +
                        "\t\t\"payerCuit\": \"30688476360\",\n" +
                        "\t\t\"paymentReference\": \"665e0d31e308cd75c8eccdae\",\n" +
                        "\t\t\"preparedDate\": \"2024-06-03T15:36:41.795\",\n" +
                        "\t\t\"tefOrdenId\": \"107384225\",\n" +
                        "\t\t\"voucherNumber\": \"00117978\"\n" +
                        "\t},\n" +
                        "\t\"date\": \"2024-06-07T16:30:03.8883894\",\n" +
                        "\t\"id\": "+i+",\n" +
                        "\t\"origin\": \"PAYMENTS\",\n" +
                        "\t\"status\": \"CREATE\"\n" +
                        "}";
                // Confection: String message = "{\"data\":{\"estado\":true,\"identificadorOperacion\":\"db37e7ab-e032-4cd0-92f1-9cb217eb06ba\",\"idProceso\":3325628},\"success\":true}";
                // Configura el registro del productor
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
                // Envía el mensaje
                producer.send(record);

                System.out.println("Mensaje " + i + " enviado");

                // Agrega un pequeño retraso entre mensajes
                Thread.sleep(delay);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Cierra el productor después de enviar los mensajes
            producer.close();
        }
    }

    // Método para crear y configurar el productor de Kafka
    private Producer<String, String> createProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "url kafka");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        

        return new KafkaProducer<>(properties);
    }
}
