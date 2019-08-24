package com.subrato.packages.solace.MessagingAppWithQueuePesistance.config;

import com.solacesystems.jcsmp.*;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

public class Subscriber {

    private ConsumerFlowProperties flow_prop = null;
    private FlowReceiver cons = null;
    private EndpointProperties endpoint_props = null;
    private ArrayList<String> messageList = null;

    public void subscribe(Queue queue, JCSMPSession session, CountDownLatch latch) throws JCSMPException{
        flow_prop = new ConsumerFlowProperties();
        flow_prop.setEndpoint(queue);
        flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);

        endpoint_props = new EndpointProperties();
        endpoint_props.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);

        messageList = new ArrayList<String>();
        cons = session.createFlow(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage msg) {
                if (msg instanceof TextMessage) {
                    System.out.printf("TextMessage received: '%s'%n", ((TextMessage) msg).getText());
                    messageList.add(((TextMessage) msg).getText());
                } else {
                    System.out.println("Message received.");
                }
                System.out.printf("Message Dump:%n%s%n", msg.dump());
                // When the ack mode is set to SUPPORTED_MESSAGE_ACK_CLIENT,
                // guaranteed delivery messages are acknowledged after
                // processing
                msg.ackMessage();
                latch.countDown(); // unblock main thread
            }

            @Override
            public void onException(JCSMPException e) {
                System.out.printf("Consumer received exception: %s%n", e);
                latch.countDown(); // unblock main thread
            }
        }, flow_prop, endpoint_props);

        cons.start();

        try {
            latch.await(); // block here until message received, and latch will flip
        } catch (InterruptedException e) {
            System.out.println("I was awoken while waiting");
        }
    }

    public String getMesssages() {
        StringBuilder sb = new StringBuilder();

        sb.append("[RECONCILE MESSAGE] : ");
        sb.append(System.getProperty("line.separator"));

        while (!messageList.isEmpty()) {
            sb.append(messageList.remove(0));
            sb.append(System.getProperty("line.separator"));
        }

        return sb.toString();
    }
}
