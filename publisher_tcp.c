/**
 * =================================================================================================
 * PROYECTO: Sistema de Publicación-Suscripción para Noticias Deportivas en Tiempo Real
 * ARCHIVO:  publisher_tcp.c
 * ROL:      Publicador (Periodista deportivo reportando incidencias en vivo)
 *
 * JUSTIFICACIÓN TÉCNICA DEL PROTOCOLO (TCP - SOCK_STREAM):
 * -------------------------------------------------------------------------------------------------
 * 1. Confiabilidad y Entrega Garantizada:
 *    TCP implementa acuses de recibo (ACKs), temporizadores de retransmisión y números de secuencia.
 *    En un contexto de noticias deportivas críticas (ej. un gol anulado, tarjeta roja o marcador final),
 *    la pérdida de un mensaje provocaría inconsistencia severa en la información que leen los hinchas.
 * 
 * 2. Mantenimiento del Orden Cronológico:
 *    El desarrollo de un partido es estrictamente secuencial. Si un gol (Minuto 32) llega antes de la
 *    falta que lo originó (Minuto 31), la experiencia del usuario se degrada. TCP asegura que el stream
 *    de bytes se entregue a la capa de aplicación del Broker en el orden exacto en que fue transmitido.
 *
 * 3. Control de Flujo y Congestión:
 *    Mediante ventanas deslizantes y algoritmos de control de congestión (Slow Start, Congestion Avoidance),
 *    TCP regula dinámicamente la tasa de envío evitando saturar los buffers del Broker o los enlaces intermedios.
 *
 * 4. Costo / Overhead:
 *    Requiere un saludo de tres vías (3-Way Handshake) para iniciar la sesión (SYN, SYN-ACK, ACK) y un
 *    cierre de 4 vías (FIN, ACK, FIN, ACK). Las cabeceras mínimas son de 20 bytes (frente a 8 de UDP).
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
#define DEFAULT_BROKER_PORT 8080
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
    printf("   PUBLISHER TCP - TRANSMISIÓN DE NOTICIAS DEPORTIVAS EN VIVO   \n");
    printf("================================================================\n");
    printf("[CONFIG] Destino Broker -> IP: %s | Puerto: %d\n", broker_ip, broker_port);
    printf("[CONFIG] Total de eventos a transmitir: %d\n\n", total_eventos);

    /**
     * PASO 1: Creación del Socket TCP (SOCK_STREAM)
     * ---------------------------------------------------------------------------------------------
     * A nivel de red:
     * - AF_INET: Familia de direcciones IPv4.
     * - SOCK_STREAM: Solicita un socket orientado a la conexión que utiliza el protocolo TCP.
     * - Protocolo 0: Permite que el sistema elija automáticamente el protocolo adecuado (IPPROTO_TCP).
     * El kernel del SO asigna un descriptor de archivo y reserva las estructuras internas necesarias
     * (colas de transmisión y recepción, buffers de retransmisión).
     */
    client_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (client_socket < 0) {
        perror("[ERROR] Falló la creación del socket TCP");
        return EXIT_FAILURE;
    }
    printf("[OK] Socket TCP creado exitosamente (descriptor: %d).\n", client_socket);

    /**
     * PASO 2: Configuración de la estructura sockaddr_in del Broker
     * ---------------------------------------------------------------------------------------------
     * A nivel de red:
     * - sin_family: Especifica IPv4.
     * - htons(): Convierte el número de puerto de formato Host Byte Order (usualmente little-endian
     *   en arquitecturas x86/ARM) a Network Byte Order (big-endian), estándar para cabeceras TCP/IP.
     * - inet_pton(): Convierte la dirección IP en formato texto con puntos a su representación binaria
     *   en orden de red de 32 bits (in_addr).
     */
    memset(&broker_addr, 0, sizeof(broker_addr));
    broker_addr.sin_family = AF_INET;
    broker_addr.sin_port = htons(broker_port);

    if (inet_pton(AF_INET, broker_ip, &broker_addr.sin_addr) <= 0) {
        fprintf(stderr, "[ERROR] Dirección IP inválida o no soportada: %s\n", broker_ip);
        close(client_socket);
        return EXIT_FAILURE;
    }

    /**
     * PASO 3: Establecimiento de la conexión mediante connect() (3-Way Handshake)
     * ---------------------------------------------------------------------------------------------
     * A nivel de red:
     * - La llamada connect() inicia activamente el saludo de tres vías de TCP:
     *     1. Cliente -> Servidor (Broker): Segmento con bandera SYN=1 y número de secuencia inicial (ISN_c).
     *     2. Servidor -> Cliente: Segmento SYN=1, ACK=1 reconociendo ISN_c + 1 y enviando ISN_s.
     *     3. Cliente -> Servidor: Segmento ACK=1 reconociendo ISN_s + 1.
     * - La llamada es bloqueante por defecto; se desbloquea únicamente cuando la conexión pasa
     *   al estado ESTABLISHED. Visible claramente en capturas de Wireshark.
     */
    printf("[INFO] Conectando al Broker en %s:%d (Iniciando 3-Way Handshake)...\n", broker_ip, broker_port);
    if (connect(client_socket, (struct sockaddr *)&broker_addr, sizeof(broker_addr)) < 0) {
        perror("[ERROR] Falló la conexión con el Broker");
        close(client_socket);
        return EXIT_FAILURE;
    }
    printf("[OK] Conexión TCP establecida con éxito con el Broker.\n\n");

    /**
     * PASO 4: Transmisión secuencial de las incidencias del partido
     * ---------------------------------------------------------------------------------------------
     * A nivel de red:
     * - Cada llamada a send() transfiere datos desde el espacio de usuario (nuestro buffer) al Send
     *   Buffer del kernel.
     * - TCP divide los datos en segmentos según el MSS (Maximum Segment Size), encapsula cada segmento
     *   con una cabecera TCP (puerto origen/destino, seq number, ack number, window size, checksum) y
     *   lo transmite sobre IP.
     * - El receptor envía ACKs que garantizan que el mensaje llegó sin corrupción ni pérdidas.
     */
    for (int i = 0; i < total_eventos; i++) {
        /* Formateamos el mensaje incluyendo salto de línea delimitador para protocolos de flujo */
        snprintf(buffer, sizeof(buffer), "%s\n", cronograma_partido[i].evento);
        size_t longitud_mensaje = strlen(buffer);

        ssize_t bytes_enviados = send(client_socket, buffer, longitud_mensaje, 0);
        if (bytes_enviados < 0) {
            perror("[ERROR] Falló el envío del mensaje a través de TCP");
            break;
        }

        printf("[ENVÍO %02d/%02d] (%zd bytes) -> %s", 
               i + 1, total_eventos, bytes_enviados, buffer);
        fflush(stdout);

        /* Pausa entre mensajes para facilitar el análisis del tráfico en Wireshark */
        sleep(DELAY_SECONDS);
    }

    /**
     * PASO 5: Cierre ordenado de la conexión TCP mediante close() (4-Way Teardown)
     * ---------------------------------------------------------------------------------------------
     * A nivel de red:
     * - close() inicia el cierre de la conexión enviando un segmento TCP con la bandera FIN=1.
     * - Se ejecuta el intercambio de 4 vías (FIN -> ACK, FIN -> ACK) para cerrar ambos sentidos
     *   del flujo full-duplex de manera limpia.
     * - El socket transita por estados como FIN_WAIT_1, FIN_WAIT_2 y TIME_WAIT en el kernel.
     */
    printf("\n[INFO] Finalizó la transmisión de incidencias. Cerrando socket TCP...\n");
    if (close(client_socket) < 0) {
        perror("[ERROR] Error al cerrar el socket");
        return EXIT_FAILURE;
    }

    printf("[OK] Socket cerrado limpiamente. Sesión TCP finalizada.\n");
    return EXIT_SUCCESS;
}
