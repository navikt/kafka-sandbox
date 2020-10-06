module clients {
    requires no.nav.kafka.sandbox.messages;
    requires org.slf4j;
    requires kafka.clients;
    requires com.fasterxml.jackson.annotation;
    requires com.fasterxml.jackson.datatype.jdk8;
    requires com.fasterxml.jackson.datatype.jsr310;
    requires com.fasterxml.jackson.databind;
}
