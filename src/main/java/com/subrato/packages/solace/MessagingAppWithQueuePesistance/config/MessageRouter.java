package com.subrato.packages.solace.MessagingAppWithQueuePesistance.config;

import com.solacesystems.jcsmp.*;
import com.subrato.packages.solace.MessagingAppWithQueuePesistance.pojos.InitializerPayload;

public class MessageRouter {

    private JCSMPProperties properties;
    private JCSMPSession session = null;
    private String queueName = null;
    private Queue queue = null;

    public MessageRouter(InitializerPayload payload) {
        properties = new JCSMPProperties();

        properties.setProperty(JCSMPProperties.HOST, payload.getHost());
        properties.setProperty(JCSMPProperties.USERNAME, payload.getUserName());
        properties.setProperty(JCSMPProperties.VPN_NAME, payload.getVpn());
        properties.setProperty(JCSMPProperties.PASSWORD, payload.getPassword());

        queueName = payload.getQueue();
        queue = JCSMPFactory.onlyInstance().createQueue(queueName);
    }

    public String connect() {
        String response = null;

        if( session == null ) {
            response = initializeSession();
        }

        if( session!=null && !session.isClosed()) {
            try {
                session.connect();
                response = "Success - Connected";
            } catch (JCSMPException e) {
                session = null;
                response = "Failed To Connect " + e.getMessage();
            }
        }

        return response;
    }

    private String initializeSession() {
        String response;

        // set queue permissions to "consume" and access-type to "exclusive"
        EndpointProperties endpointProps = new EndpointProperties();
        endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
        endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);

        try {
            session = JCSMPFactory.onlyInstance().createSession(properties);
            session.provision(queue, endpointProps, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
            response = "Success - Instance";
        } catch (InvalidPropertiesException e) {
            session = null;
            response = "Session Instance Creation Failed - " + e.getMessage();
        } catch (JCSMPException e) {
            session = null;
            response = "Session Instance Creation Failed - " + e.getMessage();
        }

        return response;
    }

    public Queue getQueue() {
        return queue;
    }

    public JCSMPSession getSession() {
        return session;
    }

}
