package pe.edu.upeu.pago.event;

public record CompraConfirmadaEvento(String tipoEvento, Long ordenId, Long estudianteId, Long cursoId, java.util.List<Long> leccionIds, String estado, String origen, long timestamp) {}
