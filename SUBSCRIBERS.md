# Subscribers — etapa 1 (10 de octubre de 2026)

Esta etapa implementa recepción preliminar. No implementa el registro de
suscripción ni confirma integración con el broker, que todavía no está disponible.
Los publishers existentes no se modificaron.

## Compilar y probar en la terminal de VS Code

```sh
mkdir -p build
clang -std=c11 -Wall -Wextra -Werror subscriber_tcp.c -o build/subscriber_tcp
clang -std=c11 -Wall -Wextra -Werror subscriber_udp.c -o build/subscriber_udp
python3 test_subscribers.py
```

TCP: `./build/subscriber_tcp 127.0.0.1 PUERTO_DEL_SERVIDOR`.
UDP: `./build/subscriber_udp PUERTO_LOCAL`.
Reemplazar los puertos por los del servidor de prueba o por los acordados con el
broker. Los puertos 8080/8081 de los publishers son destinos de publicación;
no se asume que también sean puertos para subscribers.
UDP escucha en todas las interfaces IPv4. Ctrl+C termina la recepción.

TCP conserva una línea pendiente entre lecturas, separa varias líneas recibidas
juntas y falla si el servidor cierra con una línea incompleta o si una noticia
supera 8192 bytes antes de su delimitador. UDP imprime un datagrama por vez;
no incluye filtrado de origen, reintentos ni registro de suscripción todavía.

## Evidencia preliminar

Ejecutado en macOS ARM64 con Apple clang 21.0.0. Compilación sin advertencias con
las opciones anteriores. Pruebas automatizadas aprobadas:

- TCP: 3 noticias reconstruidas correctamente a partir de envíos fragmentados y
  agrupados. No se presupone correspondencia entre send y recv.
- TCP: cierre con noticia incompleta y exceso del límite detectados como errores.
- TCP y UDP: puerto con caracteres adicionales rechazado.
- UDP: 3 datagramas locales (incluido uno vacío) recibidos en el orden enviado.

Son resultados de un servidor de prueba en loopback, sin capturas Wireshark ni
fallos de red inducidos. No prueban entrega garantizada de UDP ni rendimiento del
sistema del grupo. El test usa puertos efímeros y no ejecuta los publishers.

## Pendientes para el acuerdo del grupo

1. Puerto de subscribers y cómo distinguirlos de publishers.
2. Identificador de partido, formato exacto de suscripción y confirmación/error.
3. Delimitación de noticias TCP: los publishers actuales envían salto de línea.
4. Registro de dirección/puerto UDP, baja y duración de la suscripción.
5. Identificador único por noticia y por publisher para medir pérdidas,
   duplicados y orden por emisor. El minuto del partido no es un identificador.

## Tabla comparativa por completar

| Medición del sistema del grupo | TCP | UDP |
|---|---|---|
| Noticias enviadas por cada publisher | Pendiente | Pendiente |
| Noticias recibidas por cada subscriber | Pendiente | Pendiente |
| Pérdidas y duplicados según IDs | Pendiente | Pendiente |
| Orden por publisher | Pendiente | Pendiente |
| Latencia con relojes sincronizados o captura común | Pendiente | Pendiente |
| Tráfico de establecimiento, cierre y retransmisiones en captura | Pendiente | Pendiente |
| Comportamiento ante desconexión o pérdida inducida | Pendiente | Pendiente |

Para cada transporte, ejecutar un broker, dos publishers y dos subscribers.
Registrar entorno, comandos, IDs esperados y recibidos, logs y captura. En Mac,
seleccionar loopback para pruebas locales en Wireshark; el filtro dependerá de
los puertos acordados. Comparar primero condiciones iguales y luego fallos
controlados. Conservar evidencia por ejecución antes de completar la tabla.

## Historial de esta etapa

- Commit 1: receptores preliminares TCP/UDP y exclusión de binarios.
- Commit 2: pruebas reproducibles, resultados y pendientes de integración.
