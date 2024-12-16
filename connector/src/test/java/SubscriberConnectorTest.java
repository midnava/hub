public class SubscriberConnectorTest {

    public static void main(String[] args) throws InterruptedException {
        Connector subscriberConnector = new Connector(byteBuf -> {
            HubMessage hubMessage = MessageHubAdapter.deserializeHeader(byteBuf);

            if (hubMessage.getMsgType() == MessageType.MESSAGE) {

            } else {
                System.out.println("Received msg: " + hubMessage);
            }
        });

        subscriberConnector.start("localhost", 8080);
        subscriberConnector.subscribe("topic");

    }
}
