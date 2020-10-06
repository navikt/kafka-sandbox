module clients.spring {
    requires no.nav.kafka.sandbox.messages;
    requires spring.boot;
    requires spring.context;
    requires spring.web;
    requires spring.kafka;
    requires spring.beans;
    requires spring.boot.autoconfigure;
    requires kafka.clients;
    requires org.slf4j;
}
