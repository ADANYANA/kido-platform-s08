package pe.edu.upeu.pago.messaging;
import org.junit.jupiter.api.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import pe.edu.upeu.pago.event.CompraConfirmadaEvento;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;
class CompraEventosProducerTest {
 @AfterEach void clean(){if(TransactionSynchronizationManager.isSynchronizationActive())TransactionSynchronizationManager.clearSynchronization();TransactionSynchronizationManager.setActualTransactionActive(false);}
 @Test void publishesOnlyAfterCommitWithOrderKey(){
   @SuppressWarnings("unchecked") KafkaTemplate<String,CompraConfirmadaEvento> template=mock(KafkaTemplate.class);
   var producer=new CompraEventosProducer(template);ReflectionTestUtils.setField(producer,"topic","kido-compra-eventos");
   var event=new CompraConfirmadaEvento("compra.confirmada",42L,7L,2L,List.of(1L),"APROBADA","kido-pago-ms",123L);
   var metadata=new RecordMetadata(new TopicPartition("kido-compra-eventos",0),0,0,123L,0,0);
   when(template.send("kido-compra-eventos","42",event)).thenReturn(CompletableFuture.completedFuture(new SendResult<>(null,metadata)));
   TransactionSynchronizationManager.initSynchronization();TransactionSynchronizationManager.setActualTransactionActive(true);
   producer.publicarTrasCommit(event);verifyNoInteractions(template);
   TransactionSynchronizationManager.getSynchronizations().forEach(TransactionSynchronization::afterCommit);
   verify(template).send("kido-compra-eventos","42",event);
 }
 @Test void rollbackNeverPublishes(){
   @SuppressWarnings("unchecked") KafkaTemplate<String,CompraConfirmadaEvento> template=mock(KafkaTemplate.class);
   TransactionSynchronizationManager.initSynchronization();TransactionSynchronizationManager.setActualTransactionActive(true);
   new CompraEventosProducer(template).publicarTrasCommit(new CompraConfirmadaEvento("compra.confirmada",42L,7L,2L,List.of(),"APROBADA","kido-pago-ms",123L));
   TransactionSynchronizationManager.getSynchronizations().forEach(s->s.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK));
   verifyNoInteractions(template);
 }
 @Test void rejectsPublicationWithoutTransaction(){
   @SuppressWarnings("unchecked") KafkaTemplate<String,CompraConfirmadaEvento> template=mock(KafkaTemplate.class);
   assertThrows(IllegalStateException.class,()->new CompraEventosProducer(template).publicarTrasCommit(null));verifyNoInteractions(template);
 }
}
