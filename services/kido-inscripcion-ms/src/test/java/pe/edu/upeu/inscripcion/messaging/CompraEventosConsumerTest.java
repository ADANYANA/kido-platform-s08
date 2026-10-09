package pe.edu.upeu.inscripcion.messaging;
import org.junit.jupiter.api.Test;
import pe.edu.upeu.inscripcion.event.CompraConfirmadaEvento;
import pe.edu.upeu.inscripcion.dto.*;
import pe.edu.upeu.inscripcion.service.InscripcionService;
import java.time.LocalDateTime;
import java.util.List;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;
class CompraEventosConsumerTest {
 @Test void createsFromEventSnapshotAndCorrelatesReply(){
   var service=mock(InscripcionService.class);var producer=mock(InscripcionEventosProducer.class);
   when(service.crearPorCompra(new CompraInscripcionRequest(7L,2L,List.of(5L,6L)))).thenReturn(new InscripcionResponse(9L,7L,2L,"COMPRA","ACTIVA",LocalDateTime.now(),0,List.of()));
   new CompraEventosConsumer(service,producer).recibir(new CompraConfirmadaEvento("compra.confirmada",42L,7L,2L,List.of(5L,6L),"APROBADA","kido-pago-ms",123L));
   verify(service).crearPorCompra(new CompraInscripcionRequest(7L,2L,List.of(5L,6L)));
   verify(producer).publicarTrasCommit(argThat(e->e.ordenId().equals(42L)&&e.inscripcionId().equals(9L)&&e.tipoEvento().equals("inscripcion.creada")));
 }
 @Test void pendingPaymentCannotCreateAccess(){
   var service=mock(InscripcionService.class);var producer=mock(InscripcionEventosProducer.class);
   assertThrows(IllegalArgumentException.class,()->new CompraEventosConsumer(service,producer).recibir(new CompraConfirmadaEvento("compra.confirmada",42L,7L,2L,List.of(),"PENDIENTE","kido-pago-ms",123L)));
   verifyNoInteractions(service,producer);
 }
}
