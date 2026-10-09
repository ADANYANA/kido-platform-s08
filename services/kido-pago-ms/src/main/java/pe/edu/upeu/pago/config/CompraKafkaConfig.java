package pe.edu.upeu.pago.config;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
@Configuration
public class CompraKafkaConfig {
 @Bean public NewTopic compraTopic(@Value("${app.kafka.topic.compras}") String topic) {return TopicBuilder.name(topic).partitions(3).replicas(1).build();}
 @Bean public NewTopic inscripcionTopic(@Value("${app.kafka.topic.inscripciones}") String topic) {return TopicBuilder.name(topic).partitions(3).replicas(1).build();}
 @Bean public DefaultErrorHandler compraErrorHandler() {
   var handler=new DefaultErrorHandler(new FixedBackOff(3000L,FixedBackOff.UNLIMITED_ATTEMPTS));
   handler.setClassifications(java.util.Map.of(),true);
   return handler;
 }
}
