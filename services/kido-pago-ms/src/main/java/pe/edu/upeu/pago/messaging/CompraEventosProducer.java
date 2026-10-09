package pe.edu.upeu.pago.messaging;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import pe.edu.upeu.pago.event.CompraConfirmadaEvento;
import java.util.concurrent.TimeUnit;

@Component @RequiredArgsConstructor @Slf4j
public class CompraEventosProducer {
 private final KafkaTemplate<String, CompraConfirmadaEvento> template;
 @Value("${app.kafka.topic.compras}") private String topic;
 public void publicarTrasCommit(CompraConfirmadaEvento evento) {
   if (!TransactionSynchronizationManager.isActualTransactionActive())
     throw new IllegalStateException("La publicacion requiere una transaccion activa");
   TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
     @Override public void afterCommit() { enviar(evento); }
   });
 }
 private void enviar(CompraConfirmadaEvento e) {
   try {
     var result=template.send(topic,String.valueOf(e.ordenId()),e).get(15,TimeUnit.SECONDS);
     var m=result.getRecordMetadata();
     log.info("component=producer topic={} partition={} offset={} eventType={} ordenId={} status=published",topic,m.partition(),m.offset(),e.tipoEvento(),e.ordenId());
   } catch (InterruptedException ex) {
     Thread.currentThread().interrupt(); throw new IllegalStateException("Publicacion interrumpida tras commit",ex);
   } catch (Exception ex) {
     log.error("component=producer topic={} eventType={} ordenId={} status=error",topic,e.tipoEvento(),e.ordenId(),ex);
     throw new IllegalStateException("No se pudo publicar tras commit; reintentar la sincronizacion",ex);
   }
 }
}
