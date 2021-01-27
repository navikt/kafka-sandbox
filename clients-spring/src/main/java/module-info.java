open module clients.spring {
    requires no.nav.kafka.sandbox.messages;
    requires spring.boot;
    requires org.apache.tomcat.embed.core; // This is required for auto configuration of spring web to work with modular app..
    requires spring.boot.autoconfigure;
    requires spring.context;
    requires spring.core;
    requires spring.web;
    requires spring.kafka;
    requires spring.beans;
    requires kafka.clients;
    requires com.fasterxml.jackson.databind;
    requires org.slf4j;
}
