package pe.edu.upeu.pago.messaging;
import org.junit.jupiter.api.Test;
import pe.edu.upeu.pago.entity.OrdenCompra;
import pe.edu.upeu.pago.repository.OrdenCompraRepository;
import pe.edu.upeu.pago.event.InscripcionCreadaEvento;
import java.util.Optional;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;
class InscripcionEventosConsumerTest {
 private OrdenCompra order(OrdenCompra.EstadoOrden state){var o=new OrdenCompra();o.setId(42L);o.setEstudianteId(7L);o.setCursoId(2L);o.setEstado(state);return o;}
 private InscripcionCreadaEvento event(long student){return new InscripcionCreadaEvento("inscripcion.creada",42L,9L,student,2L,"ACTIVA","kido-inscripcion-ms",123L);}
 @Test void confirmationUpdatesOrderAndDuplicateIsHarmless(){
   var repo=mock(OrdenCompraRepository.class);var o=order(OrdenCompra.EstadoOrden.APROBADA);when(repo.findById(42L)).thenReturn(Optional.of(o));var c=new InscripcionEventosConsumer(repo);
   c.recibir(event(7));c.recibir(event(7));assertTrue(o.isInscripcionSincronizada());verify(repo,times(2)).save(o);
 }
 @Test void mismatchedStudentCannotUpdateOrder(){
   var repo=mock(OrdenCompraRepository.class);var o=order(OrdenCompra.EstadoOrden.APROBADA);when(repo.findById(42L)).thenReturn(Optional.of(o));
   assertThrows(IllegalArgumentException.class,()->new InscripcionEventosConsumer(repo).recibir(event(8)));assertFalse(o.isInscripcionSincronizada());verify(repo,never()).save(any());
 }
 @Test void lateConfirmationCannotChangeRefundedOrder(){
   var repo=mock(OrdenCompraRepository.class);var o=order(OrdenCompra.EstadoOrden.REEMBOLSADA);when(repo.findById(42L)).thenReturn(Optional.of(o));
   new InscripcionEventosConsumer(repo).recibir(event(7));assertFalse(o.isInscripcionSincronizada());verify(repo,never()).save(any());
 }
}
