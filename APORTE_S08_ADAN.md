# Verificación individual S08

- Estudiante: Adan Yana Rojas.
- Equipo: 11.
- Curso: Desarrollo de aplicaciones distribuidas.
- Docente: Sullon Macalupu Abel Ángel.
- Base: proyecto compartido Kido.

## Proceso realizado

1. Levanté el entorno con Docker Compose.
2. Verifiqué Kafka en modo KRaft y Kafka UI.
3. Comprobé el topic kido-pago-eventos con 3 particiones y factor de réplica 1.
4. Revisé el callback afterCommit, la key ordenId y el listener del consumidor.
5. Ejecuté las pruebas de flujo normal y desacople.
6. Revisé los logs, las notificaciones y el lag del grupo.
7. Organicé las evidencias y la documentación individual.

## Flujo normal

La orden 10 quedó APROBADA. El evento pago.aprobado se publicó con key 10, partición 1 y offset 2.

El consumidor registró notifications=2. La consulta del estudiante 73054 devolvió la notificación 19 en estado ENVIADA. El lag final fue 0 en las tres particiones.

## Desacople

La orden 9 quedó APROBADA mientras el consumidor estaba apagado. Kafka conservó los eventos pendientes.

Al reiniciar el consumidor se procesaron las órdenes pendientes 6, 7, 8 y 9, y el lag volvió a 0.

## Hallazgo

La consulta de notificaciones devolvió HTTP 500 porque el Gateway no encontraba una instancia disponible de kido-notificacion-ms.

El pago y el evento se conservaron. Después de recuperar el consumidor, los pendientes se procesaron y una nueva prueba normal terminó correctamente.

## Reflexión

Comprobé que el pago puede aprobarse cuando el consumidor está apagado.
Kafka conservó los eventos hasta que el servicio volvió a estar disponible.
Relacioné publicación y procesamiento mediante ordenId.
El lag permitió observar los mensajes pendientes.
La consulta HTTP de notificaciones requirió un servicio disponible.
Los logs me ayudaron a identificar el fallo y verificar la recuperación.

## Alcance

Esta entrega documenta mi ejecución, revisión y presentación individual sobre el proyecto compartido Kido. Los componentes previos conservan la autoría de sus colaboradores.
