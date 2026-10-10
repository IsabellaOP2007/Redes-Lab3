/**
 * =================================================================================================
 * PROYECTO: Sistema de Publicación-Suscripción para Noticias Deportivas en Tiempo Real
 * ARCHIVO:  publisher_udp.c
 * ROL:      Publicador (Periodista deportivo reportando incidencias en vivo)
 *
 * JUSTIFICACIÓN TÉCNICA DEL PROTOCOLO (UDP - SOCK_DGRAM):
 * -------------------------------------------------------------------------------------------------
 * 1. Ausencia de Estado y Sin Conexión (Connectionless):
 *    UDP no requiere establecer una sesión previa mediante handshake. Cada mensaje se envía
 *    directamente como un datagrama autónomo. Esto elimina el retardo de inicio (Round-Trip Time, RTT)
 *    de la conexión, permitiendo envíos inmediatos.
 *
 * 2. Mínimo Overhead de Protocolo:
 *    La cabecera de UDP es fija y consta de únicamente 8 bytes:
 *    - Puerto de Origen (16 bits)
 *    - Puerto de Destino (16 bits)
 *    - Longitud del datagrama (16 bits)
 *    - Checksum (16 bits)
 *    En comparación con los 20 bytes mínimos de TCP (más opciones como SACK, Timestamps), UDP ahorra
 *    ancho de banda y procesamiento en el kernel.
 *
 * 3. Independencia de Mensajes (Datagramas):
 *    A diferencia de TCP que es un flujo continuo de bytes (stream), UDP preserva los límites de los
 *    mensajes. Cada llamada a sendto() genera exactamente un paquete IP indivisible en la aplicación.
 *
 * 4. Implicaciones y Desventajas en el Contexto Deportivo:
 *    - No Confiable (Best-Effort): No hay ACKs ni retransmisiones automáticas. Si un paquete con un
 *      "Gol" se pierde en la red, el Broker y los hinchas nunca se enterarán a menos que la capa de
 *      aplicación implemente su propio mecanismo de recuperación.
 *    - Sin Garantía de Orden: Los datagramas pueden tomar rutas distintas y llegar desordenados
 *      (ej. recibir primero el marcador 2-1 antes del 1-1).
 *    - Sin Control de Congestión: El emisor puede saturar la red si no modula manualmente la tasa de envío.
 * =================================================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>

#define DEFAULT_BROKER_IP   "127.0.0.1"
#define DEFAULT_BROKER_PORT 8081
#define BUFFER_SIZE         512
#define DELAY_SECONDS       2  /* Pausa entre eventos para simular tiempo real y facilitar captura en Wireshark */

/* Estructura para modelar eventos deportivos de un partido */
typedef struct {
    int minuto;
    const char *evento;
} EventoDeportivo;

/* Lista de al menos 10 incidencias secuenciales de un partido de fútbol */
static const EventoDeportivo cronograma_partido[] = {
    {  1, "[PARTIDO: Real Madrid vs FC Barcelona] Minuto 01: ¡Rueda el balón! Arranca el clásico en el Santiago Bernabéu." },
    { 12, "[PARTIDO: Real Madrid vs FC Barcelona] Minuto 12: Remate peligroso de Vinicius Jr. que se va rozando el poste derecho." },
    { 24, "[PARTIDO: Real Madrid vs FC Barcelona] Minuto 24: Tarjeta amarilla para el defensor número 4 por infracción táctica." },
    { 35, "[PARTIDO: Real Madrid vs FC Barcelona] Minuto 35: ¡GOOOOOOL de Real Madrid! Vinicius Jr. define cruzado tras contraataque (1-0)." },
    { 45, "[PARTIDO: Real Madrid vs FC Barcelona] Minuto 45: El árbitro añade 2 minutos de tiempo de descuento a la primera mitad." },
    { 45, "[PARTIDO: Real Madrid vs FC Barcelona] Minuto 45+2: Final del primer tiempo. Marcador parcial: Real Madrid 1 - 0 FC Barcelona." },
    { 46, "[PARTIDO: Real Madrid vs FC Barcelona] Minuto 46: Inicia la segunda parte sin modificaciones en las alineaciones." },
    { 58, "[PARTIDO: Real Madrid vs FC Barcelona] Minuto 58: Cambio en FC Barcelona: entra jugador 10 por jugador 20 buscando más ataque." },
    { 67, "[PARTIDO: Real Madrid vs FC Barcelona] Minuto 67: ¡GOOOOOOL de FC Barcelona! Remate de media distancia inatajable para el arquero (1-1)." },
    { 79, "[PARTIDO: Real Madrid vs FC Barcelona] Minuto 79: Tarjeta roja directa para el mediocampista número 8 por falta como último recurso." },
    { 88, "[PARTIDO: Real Madrid vs FC Barcelona] Minuto 88: ¡GOOOOOOL agónico de FC Barcelona! Cabezazo al ángulo en tiro de esquina (1-2)." },
    { 90, "[PARTIDO: Real Madrid vs FC Barcelona] Minuto 90+4: ¡Pitazo final! FC Barcelona remonta y vence 2-1 en un partidazo." }
};

int main(int argc, char *argv[]) {
    const char *broker_ip = DEFAULT_BROKER_IP;
    int broker_port = DEFAULT_BROKER_PORT;
    int client_socket = -1;
    struct sockaddr_in broker_addr;
    char buffer[BUFFER_SIZE];
    int total_eventos = sizeof(cronograma_partido) / sizeof(cronograma_partido[0]);

    /* Lectura de argumentos por línea de comandos si se proporcionan */
    if (argc >= 2) {
        broker_ip = argv[1];
    }
    if (argc >= 3) {
        broker_port = atoi(argv[2]);
        if (broker_port <= 0 || broker_port > 65535) {
            fprintf(stderr, "[ERROR] Puerto inválido: %s. Debe estar entre 1 y 65535.\n", argv[2]);
            return EXIT_FAILURE;
        }
    }

    printf("================================================================\n");
    printf("   PUBLISHER UDP - TRANSMISIÓN DE NOTICIAS DEPORTIVAS EN VIVO   \n");
    printf("================================================================\n");
    printf("[CONFIG] Destino Broker -> IP: %s | Puerto: %d\n", broker_ip, broker_port);
    printf("[CONFIG] Total de eventos a transmitir: %d\n\n", total_eventos);

    /**
     * PASO 1: Creación del Socket UDP (SOCK_DGRAM)
     * ---------------------------------------------------------------------------------------------
     * A nivel de red:
     * - AF_INET: Familia de direcciones de red IPv4.
     * - SOCK_DGRAM: Solicita un socket no orientado a conexión que utiliza el protocolo UDP.
     * - Protocolo 0: Permite que el sistema seleccione el protocolo por defecto asociado (IPPROTO_UDP).
     * En UDP no se reservan buffers de conexión activa ni estados de retransmisión TCP (no hay TCB -
     * Transmission Control Block), lo que genera un impacto de memoria mínimo en el sistema operativo.
     */
    client_socket = socket(AF_INET, SOCK_DGRAM, 0);
    if (client_socket < 0) {
        perror("[ERROR] Falló la creación del socket UDP");
        return EXIT_FAILURE;
    }
    printf("[OK] Socket UDP creado exitosamente (descriptor: %d).\n", client_socket);

    /**
     * PASO 2: Configuración de la dirección de destino del Broker
     * ---------------------------------------------------------------------------------------------
     * A nivel de red:
     * - sin_family = AF_INET: Indica que la dirección pertenece a la familia IPv4.
     * - htons(): Transforma el entero del puerto de Host Byte Order a Network Byte Order (Big-Endian).
     * - inet_pton(): Convierte la cadena en notación decimal con puntos (ej. "127.0.0.1") a su
     *   representación numérica binaria requerida por la capa IP.
     * NOTA CLAVE: En UDP NO se ejecuta connect(). La dirección configurada aquí no negocia nada con el
     * Broker; se proporciona directamente en cada llamada de envío.
     */
    memset(&broker_addr, 0, sizeof(broker_addr));
    broker_addr.sin_family = AF_INET;
    broker_addr.sin_port = htons(broker_port);

    if (inet_pton(AF_INET, broker_ip, &broker_addr.sin_addr) <= 0) {
        fprintf(stderr, "[ERROR] Dirección IP inválida o no soportada: %s\n", broker_ip);
        close(client_socket);
        return EXIT_FAILURE;
    }

    printf("[INFO] Listo para enviar datagramas UDP hacia %s:%d (sin handshake previo).\n\n", 
           broker_ip, broker_port);

    /**
     * PASO 3: Envío secuencial de datagramas independientes con sendto()
     * ---------------------------------------------------------------------------------------------
     * A nivel de red:
     * - sendto() recibe explícitamente la dirección y tamaño de la estructura de destino (broker_addr).
     * - El kernel encapsula inmediatamente el payload con una cabecera UDP de 8 bytes y una cabecera
     *   IP de 20 bytes, inyectándolo directamente al adaptador de red.
     * - No hay confirmación (ACK) de si el Broker está escuchando, encendido o si el puerto está abierto.
     *   (Si el puerto está cerrado, el host receptor podría generar un mensaje ICMP 'Port Unreachable',
     *   pero UDP por sí mismo no verifica la recepción).
     * - Cada mensaje viaja como un paquete aislado en la red.
     */
    for (int i = 0; i < total_eventos; i++) {
        snprintf(buffer, sizeof(buffer), "%s\n", cronograma_partido[i].evento);
        size_t longitud_mensaje = strlen(buffer);

        ssize_t bytes_enviados = sendto(
            client_socket,
            buffer,
            longitud_mensaje,
            0,
            (struct sockaddr *)&broker_addr,
            sizeof(broker_addr)
        );

        if (bytes_enviados < 0) {
            perror("[ERROR] Falló el envío del datagrama con sendto()");
            break;
        }

        printf("[DATAGRAMA %02d/%02d] (%zd bytes) -> %s", 
               i + 1, total_eventos, bytes_enviados, buffer);
        fflush(stdout);

        /* Pausa entre paquetes para facilitar la inspección individual en Wireshark */
        sleep(DELAY_SECONDS);
    }

    /**
     * PASO 4: Cierre del socket UDP mediante close()
     * ---------------------------------------------------------------------------------------------
     * A nivel de red:
     * - A diferencia de TCP, en UDP close() NO envía ningún paquete de control por la red
     *   (no existen paquetes FIN ni ACKs de terminación).
     * - La llamada simplemente libera el descriptor de archivo y los recursos locales asignados
     *   por el kernel del sistema operativo.
     */
    printf("\n[INFO] Transmisión de datagramas completada. Cerrando socket UDP...\n");
    if (close(client_socket) < 0) {
        perror("[ERROR] Error al cerrar el socket");
        return EXIT_FAILURE;
    }

    printf("[OK] Socket cerrado exitosamente. Proceso UDP finalizado.\n");
    return EXIT_SUCCESS;
}
