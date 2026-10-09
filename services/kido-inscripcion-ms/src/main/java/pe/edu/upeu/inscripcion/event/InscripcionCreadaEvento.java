package pe.edu.upeu.inscripcion.event;

public record InscripcionCreadaEvento(String tipoEvento, Long ordenId, Long inscripcionId, Long estudianteId, Long cursoId, String estado, String origen, long timestamp) {}
