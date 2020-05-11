import java.util.*;
import java.io.FileReader;
import com.opencsv.CSVReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer kafkaProducer = new KafkaProducer(properties);
        try {
            String path = "kafka_data\\team_info.csv";
            FileReader filereader = new FileReader(path);
            CSVReader csvReader = new CSVReader(filereader);
            String[] nextRecord;
            int counter = 0;
            System.out.println("Enter the line number from which you want to publish data: ");
            int lineNumStartFrom = sc.nextInt();
            System.out.println("\nEnter the line number to which you want to publish data: ");
            int lineNumTo = sc.nextInt();
            while ((nextRecord = csvReader.readNext()) != null) {
                if (counter > lineNumStartFrom && counter <= lineNumTo) {
                    String data = Arrays.toString(nextRecord);
                    String finalData = data.substring(1, data.length()-1);
                    kafkaProducer.send(new ProducerRecord("producerTest",finalData));
                    Thread.sleep(1000);
                }
                counter++;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            kafkaProducer.close();
        }
    }
}