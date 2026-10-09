param([switch]$Desacople)
$ErrorActionPreference = "Stop"
$Base = "http://localhost:18080"
$token = Invoke-RestMethod -Method Post -Uri "http://localhost:19090/realms/kido/protocol/openid-connect/token" -ContentType "application/x-www-form-urlencoded" -Body @{client_id="kido-cli";grant_type="password";username="admin";password="Admin123!"}
$headers = @{Authorization="Bearer $($token.access_token)"}
function Crear-Compra {
    $student = Get-Random -Minimum 200000 -Maximum 900000
    $orden = Invoke-RestMethod -Method Post -Uri "$Base/api/v1/pagos/ordenes" -Headers $headers -ContentType "application/json" -Body (@{estudianteId=$student;cursoId=2} | ConvertTo-Json)
    Invoke-RestMethod -Method Post -Uri "$Base/api/v1/pagos/ordenes/$($orden.id)/simular-aprobacion" -Headers $headers
}
function Esperar-Confirmacion($id) {
    $limite=(Get-Date).AddSeconds(120)
    do {
        try {
            $o=Invoke-RestMethod -Uri "$Base/api/v1/pagos/ordenes/$id" -Headers $headers
            if($o.inscripcionSincronizada){return $o}
        } catch { Write-Host "Esperando disponibilidad del Gateway..." }
        Start-Sleep -Seconds 2
    } while((Get-Date) -lt $limite)
    throw "La orden $id no recibio inscripcion.creada en 120 segundos. Revisar logs."
}
function Mostrar-Lag {
    foreach($grupo in @("kido-inscripcion-ms","kido-pago-ms")) {
        docker exec kido-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kido-kafka:9092 --group $grupo --describe
        if($LASTEXITCODE -ne 0){throw "No se pudo consultar el grupo $grupo"}
    }
}
Write-Host "=== KIDO S08: DOS EVENTOS DE IDA Y VUELTA ===" -ForegroundColor Cyan
Write-Host "Preparando ambos consumer groups con una compra de control..."
$control=Crear-Compra
$confirmada=Esperar-Confirmacion $control.id
$confirmada | ConvertTo-Json -Depth 6
if($Desacople) {
    try {
        docker compose stop kido-inscripcion-ms
        if($LASTEXITCODE -ne 0){throw "No se pudo detener Inscripcion"}
        $orden=Crear-Compra
        if($orden.estado -ne "APROBADA" -or $orden.inscripcionSincronizada){throw "Estado inesperado con Inscripcion apagada"}
        Write-Host "CAPTURA A: APROBADA + inscripcionSincronizada=false + servicio apagado + lag pendiente" -ForegroundColor Yellow
        $orden | ConvertTo-Json -Depth 6
        docker compose ps -a kido-inscripcion-ms kido-pago-ms kido-kafka
        Start-Sleep -Seconds 3
        Mostrar-Lag
        Read-Host "Toma la captura con reloj y perfil visibles. ENTER para recuperar Inscripcion"
    } finally {
        docker compose up -d kido-inscripcion-ms
        if($LASTEXITCODE -ne 0){throw "No se pudo reiniciar Inscripcion; ejecutar docker compose up -d kido-inscripcion-ms"}
    }
} else { $orden=Crear-Compra }
$final=Esperar-Confirmacion $orden.id
Write-Host "CAPTURA B: confirmacion recibida por Kafka" -ForegroundColor Green
$final | ConvertTo-Json -Depth 6
Write-Host "Logs correlacionados con ordenId=$($orden.id):"
docker compose logs --no-color --since=15m kido-pago-ms kido-inscripcion-ms | Select-String -Pattern "ordenId=$($orden.id)\b"
Start-Sleep -Seconds 5
Mostrar-Lag
Write-Host "Consulta de notificaciones del evento adicional pago.aprobado:"
$ok=$false
for($n=0;$n -lt 30;$n++) {
    try {
        $avisos=Invoke-RestMethod -Uri "$Base/api/v1/notificaciones/usuario/$($orden.estudianteId)" -Headers $headers
        if(@($avisos).Count -gt 0){$avisos | ConvertTo-Json -Depth 8;$ok=$true;break}
    } catch { Write-Host "Esperando Notificacion y registro en Eureka..." }
    Start-Sleep -Seconds 2
}
if(!$ok){throw "Falta comprobar las notificaciones. Revisar kido-notificacion-ms"}
Write-Host "Orden: $($orden.id); estudiante: $($orden.estudianteId). Kafka UI: http://localhost:18085"
