package pe.edu.upeu.pago.messaging;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import pe.edu.upeu.pago.entity.OrdenCompra;
import pe.edu.upeu.pago.event.InscripcionCreadaEvento;
import pe.edu.upeu.pago.repository.OrdenCompraRepository;

@Component @RequiredArgsConstructor @Slf4j
public class InscripcionEventosConsumer {
 private final OrdenCompraRepository repository;
 @Transactional
 @KafkaListener(topics="${app.kafka.topic.inscripciones}")
 public void recibir(InscripcionCreadaEvento e) {
   if(!"inscripcion.creada".equals(e.tipoEvento()) || !"ACTIVA".equals(e.estado()) || e.inscripcionId()==null)
     throw new IllegalArgumentException("Confirmacion de inscripcion invalida");
   var o=repository.findById(e.ordenId()).orElseThrow(()->new IllegalArgumentException("Orden inexistente"));
   if(!o.getEstudianteId().equals(e.estudianteId()) || !o.getCursoId().equals(e.cursoId()))
     throw new IllegalArgumentException("La confirmacion no corresponde a la orden");
   if(o.getEstado()!=OrdenCompra.EstadoOrden.APROBADA) {
     log.warn("component=consumer eventType={} ordenId={} status=ignored estado={}",e.tipoEvento(),e.ordenId(),o.getEstado());
     return;
   }
   o.setInscripcionSincronizada(true); repository.save(o);
   log.info("component=consumer eventType={} ordenId={} status=consumed",e.tipoEvento(),e.ordenId());
   log.info("component=processor eventType={} ordenId={} inscripcionSincronizada=true status=processed",e.tipoEvento(),e.ordenId());
 }
}
