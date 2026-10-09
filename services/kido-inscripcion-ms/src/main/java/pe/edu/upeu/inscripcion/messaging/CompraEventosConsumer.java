package pe.edu.upeu.inscripcion.messaging;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import pe.edu.upeu.inscripcion.event.*;
import pe.edu.upeu.inscripcion.dto.CompraInscripcionRequest;
import pe.edu.upeu.inscripcion.service.InscripcionService;
import java.time.Instant;

@Component @RequiredArgsConstructor @Slf4j
public class CompraEventosConsumer {
 private final InscripcionService service;
 private final InscripcionEventosProducer producer;
 @Transactional
 @KafkaListener(topics="${app.kafka.topic.compras}")
 public void recibir(CompraConfirmadaEvento e) {
   if (!"compra.confirmada".equals(e.tipoEvento()) || !"APROBADA".equals(e.estado()))
     throw new IllegalArgumentException("Evento de compra invalido");
   if(e.ordenId()==null || e.estudianteId()==null || e.cursoId()==null || e.leccionIds()==null)
     throw new IllegalArgumentException("Contrato incompleto");
   log.info("component=consumer eventType={} ordenId={} status=consumed",e.tipoEvento(),e.ordenId());
   var i=service.crearPorCompra(new CompraInscripcionRequest(e.estudianteId(),e.cursoId(),e.leccionIds()));
   producer.publicarTrasCommit(new InscripcionCreadaEvento("inscripcion.creada",e.ordenId(),i.id(),e.estudianteId(),e.cursoId(),i.estado(),"kido-inscripcion-ms",Instant.now().toEpochMilli()));
   log.info("component=processor eventType={} ordenId={} inscripcionId={} status=processed",e.tipoEvento(),e.ordenId(),i.id());
 }
}
