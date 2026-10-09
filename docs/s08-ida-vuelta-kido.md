# S08: intercambio de eventos en Kido

Esta adaptación utiliza el proyecto compartido Kido. La orden de compra y el pago pertenecen a Pago; el acceso al curso pertenece a Inscripción. La transición equivalente a una orden pagada es `APROBADA`. La respuesta asíncrona de Inscripción cambia `inscripcionSincronizada` de `false` a `true`.

## Contratos separados

Todos los mensajes usan key igual a `String.valueOf(ordenId)`, 3 particiones y réplica 1. Cada topic contiene un único tipo de evento; no se comparten clases Java entre servicios. Los ejemplos son ilustrativos: sustituir los identificadores por los de la ejecución capturada.

### compra.confirmada

- Topic: `kido-compra-eventos`.
- Productor: `kido-pago-ms`; consumidor y grupo: `kido-inscripcion-ms`.
- Publicación después del commit del pago aprobado.

```json
{"tipoEvento":"compra.confirmada","ordenId":11,"estudianteId":200001,"cursoId":2,"leccionIds":[1,2],"estado":"APROBADA","origen":"kido-pago-ms","timestamp":1791516000000}
```

| Campo | Tipo JSON | Regla |
|---|---|---|
| tipoEvento | string | compra.confirmada |
| ordenId | integer | Orden aprobada; key y correlación |
| estudianteId | integer | Estudiante que obtiene el acceso |
| cursoId | integer | Curso comprado |
| leccionIds | array de integer | Snapshot guardado al crear la orden; puede estar vacío |
| estado | string | APROBADA |
| origen | string | kido-pago-ms |
| timestamp | integer | Momento de construcción del evento, milisegundos epoch |

Inscripción dispone de estudiante, curso y lecciones para crear el acceso sin consultar Pago ni Curso. La inscripción existente se reutiliza por estudiante y curso; un reintento de la misma compra no crea una segunda inscripción.

### inscripcion.creada

- Topic: `kido-inscripcion-eventos`.
- Productor: `kido-inscripcion-ms`; consumidor y grupo: `kido-pago-ms`.
- Publicación después del commit de la inscripción. Pago comprueba estudiante y curso contra la orden antes de marcarla sincronizada. Una respuesta para una orden ya reembolsada se ignora.

```json
{"tipoEvento":"inscripcion.creada","ordenId":11,"inscripcionId":8,"estudianteId":200001,"cursoId":2,"estado":"ACTIVA","origen":"kido-inscripcion-ms","timestamp":1791516001000}
```

| Campo | Tipo JSON | Regla |
|---|---|---|
| tipoEvento | string | inscripcion.creada |
| ordenId | integer | Key; orden a actualizar |
| inscripcionId | integer | Inscripción persistida |
| estudianteId | integer | Debe coincidir con la orden |
| cursoId | integer | Debe coincidir con la orden |
| estado | string | ACTIVA |
| origen | string | kido-inscripcion-ms |
| timestamp | integer | Construcción del evento, milisegundos epoch |

### Evento adicional: pago.aprobado

Se conserva el contrato completo en [la guía original S08](s08-mensajeria-asincrona.md). Topic `kido-pago-eventos`, productor Pago, consumidor y grupo Notificación. Su efecto independiente son los avisos para estudiante y docente. No se cuenta como uno de los dos eventos anteriores.

## Ejecución y capturas

Desde la raíz del proyecto:

```powershell
docker compose up -d --build
powershell -ExecutionPolicy Bypass -File .\scripts\demo-s8-ida-vuelta.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\demo-s8-ida-vuelta.ps1 -Desacople
powershell -ExecutionPolicy Bypass -File .\scripts\demo-s8-desacople.ps1
```

El script nuevo espera una compra de control para registrar ambos grupos antes de detener Inscripción. La prueba de desacople se pausa: capturar la orden APROBADA, sincronización false, Inscripción detenida y lag mayor que cero. ENTER la reinicia; capturar sincronización true, los dos eventos correlacionados y lag final cero. La prueba original detiene Notificación y evidencia el desacople del evento adicional.

1. Docker: Kafka, UI, Pago, Inscripción y Notificación levantados.
2. Kafka UI: los tres topics, particiones y réplica.
3. Eureka: los tres microservicios registrados.
4. Config Server: configuración externa de productor y consumidor; ocultar credenciales.
5. PostgreSQL: orden APROBADA y columna inscripcion_sincronizada en true.
6. Logs de compra.confirmada publicado/consumido y de inscripcion.creada publicado/consumido, misma ordenId.
7. Kafka UI Messages de ambos eventos: JSON, key, partition y offset.
8. Inscripción apagada: orden aprobada, sincronización false y lag pendiente; al recuperar, true y lag cero.
9. Evento adicional: pago.aprobado, notificaciones del estudiante y docente, y desacople de Notificación.
10. GitHub: commit individual con cambios concretos y fechas coherentes.

Todas las capturas deben incluir reloj/fecha del sistema y usuario/perfil visible. Los logs UTC pueden corresponder al día siguiente respecto a Lima (UTC-5); explicar esta diferencia en el informe.

## Límites técnicos y reflexión

El commit de PostgreSQL precede a la publicación; esto evita anunciar una transacción revertida. No constituye una transacción atómica PostgreSQL/Kafka: una caída entre commit y envío puede dejar la publicación pendiente. El endpoint existente de reintento de inscripción vuelve a publicar compra.confirmada. Una mejora posterior es un transactional outbox. Los listeners del intercambio conservan el offset ante fallos y reintentan; un contrato inválido requiere intervención porque bloquea su partición. Kafka desacopla la disponibilidad de Inscripción del pago, mientras que consultar las notificaciones por HTTP sigue requiriendo que Notificación esté disponible.

La migración V2 añade el snapshot sin alterar V1 ni borrar datos. Las órdenes antiguas quedan con snapshot vacío: realizar las pruebas con órdenes nuevas. Reembolsos y otros flujos HTTP permanecen fuera del intercambio agregado.

## Correspondencia con la rúbrica

Las dimensiones 1, 3, 4, 5, 6 y 7 se evidencian con infraestructura, contratos, dos pruebas de desacople, commits y reflexión. La dimensión 2 se adapta como Pago → Inscripción → Pago con una orden APROBADA y acceso confirmado. Debe explicarse esta equivalencia en el PDF; el docente determina la aceptación de la adaptación y la nota. Este documento describe el código y los pasos; no afirma resultados de pruebas aún no ejecutadas en Docker.

## Validacion de esta actualizacion

Pago e Inscripcion compilaron y generaron sus JAR con Java 21. Se ejecutaron 8 pruebas unitarias: publicacion diferida al commit, ausencia de publicacion al rollback, rechazo sin transaccion, confirmacion y repeticion, estudiante incorrecto, respuesta tardia a orden reembolsada, uso del snapshot y rechazo de compra pendiente. No sustituyen las pruebas integradas con Kafka y PostgreSQL que se ejecutan en Docker.
