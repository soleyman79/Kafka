import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        while (true) {
            Scanner scanner = new Scanner(System.in);
            System.out.println("\n" +
                    "Choose your Role\n" +
                    "1: Producer\n" +
                    "2: Consumer"
            );
            String role = scanner.nextLine();
            if (role.equals("1")) {
                System.out.println("Enter Topic:");
                String topic = scanner.nextLine();
                System.out.println("Enter Message");
                String message = scanner.nextLine();
                Producer producer = new Producer(topic, message);
                producer.produce();
            } else if (role.equals("2")) {
                System.out.println("Enter Topic:");
                String topic = scanner.nextLine();
                Consumer consumer = new Consumer(topic);
                consumer.consume();
            } else {
                System.out.println("Invalid Input");
            }
        }
    }
}
