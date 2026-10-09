# Kido Platform — S08: Mensajería asíncrona con Kafka

## Presentación individual

- Estudiante: Adan Yana Rojas.
- Equipo: 11.
- Curso: Desarrollo de aplicaciones distribuidas.
- Docente: Sullon Macalupu Abel Ángel.

Esta entrega documenta mi ejecución y verificación S08 sobre el proyecto compartido Kido.

## Mensajería

| Elemento | Valor |
|---|---|
| Evento | pago.aprobado |
| Topic | kido-pago-eventos |
| Productor | kido-pago-ms |
| Consumidor y grupo | kido-notificacion-ms |
| Key | ordenId |
| Particiones | 3 |
| Factor de réplica | 1 |
| Modo | KRaft |

El productor utiliza afterCommit para publicar después del commit. El consumidor genera notificaciones para estudiante y docente.

## Arranque

Configurar .env a partir de .env.example si todavía no existe.

    docker compose up -d --build
    docker compose ps -a

## Accesos

- Kafka UI: http://localhost:18085
- Eureka: http://localhost:18761
- Gateway: http://localhost:18080
- Keycloak: http://localhost:19090
- Config Server: http://localhost:18888

Kafka: kido-kafka:9092 dentro de Docker y localhost:19092 desde el host.

## Prueba normal

    powershell -ExecutionPolicy Bypass -File .\scripts\demo-s8.ps1

Resultado verificado: orden 10 APROBADA, evento publicado y procesado, notificación 19 ENVIADA para el estudiante 73054 y lag final 0.

## Prueba de desacople

    powershell -ExecutionPolicy Bypass -File .\scripts\demo-s8-desacople.ps1

La orden 9 quedó APROBADA con el consumidor apagado. Después del reinicio se procesaron los pendientes y el lag volvió a 0.

## Hallazgo

La consulta de notificaciones falló cuando el Gateway no encontraba una instancia disponible del servicio. El pago y el evento se conservaron. Tras recuperar el consumidor, una nueva prueba normal terminó correctamente.

## Documentación

- [Guía S08](docs/s08-mensajeria-asincrona.md)
- [Verificación individual](APORTE_S08_ADAN.md)
- [Microservicios](LISTA_MICROSERVICIOS.md)
- [Instrucciones](INSTRUCCIONES_S08.txt)

## Base compartida

Esta entrega presenta mi verificación individual. Los componentes del proyecto conservan la autoría de sus respectivos colaboradores.
