package com.subrato.packages.solace.MessagingAppWithQueuePesistance.config;

import com.solacesystems.jcsmp.*;

import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;

public class Producer {
    private LinkedList<MessageInfo> msgList = new LinkedList<MessageInfo>();
    private XMLMessageProducer prod = null;
    private CountDownLatch latch = null;
    private int id = 0;

    public void initialize(JCSMPSession session, CountDownLatch latch) throws JCSMPException {
        this.latch = latch;
        prod = session.getMessageProducer(new PublisherCallback(latch));
    }

    public String sendMsg(String message, Queue queue) {

        TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        msg.setDeliveryMode(DeliveryMode.PERSISTENT);
        msg.setText(message);

        // Delivery not yet confirmed. See ConfirmedPublish.java
        final MessageInfo msgCorrelationInfo = new MessageInfo(++id);
        msgCorrelationInfo.sessionIndependentMessage = msg;
        msgList.add(msgCorrelationInfo);
        msg.setCorrelationKey(msgCorrelationInfo);

        try {
            prod.send(msg, queue);
        } catch (JCSMPException e) {
            return "[Publishing Failed] " + e.getMessage();
        }

        try {
            latch.await(); // block here until message received, and latch will flip
        } catch (InterruptedException e) {
            System.out.println("I was awoken while waiting");
        }

        return "[Message Pushed]";
    }

    public String reconsile(){
        StringBuilder sb = new StringBuilder();

        while (msgList.peek() != null) {
            final MessageInfo ackedMsgInfo = msgList.poll();
            String resp = "Removing acknowledged message (%s) from application list.\n" + ackedMsgInfo;

            sb.append(resp);
            sb.append(System.getProperty("line.separator"));
        }

        return sb.toString();
    }

}
