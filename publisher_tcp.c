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
#define DELAY_SECONDS       2  /* Pausa entre eventos para simular tiempo real y que quede mas easy la captura en wireshark */

/* estructura para modelar un partido */
typedef struct {
    int minuto;
    const char *evento;
} EventoDeportivo;

/* lista de al menos 10 incidencias secuenciales de un partido de fútbol */
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

    /* lectura de argumentos por consola si se necesita */
    if (argc >= 2) {
        broker_ip = argv[1];
    }
    if (argc >= 3) {
        broker_port = atoi(argv[2]);
        if (broker_port <= 0 || broker_port > 65535) {
            fprintf(stderr, "Puerto no valido: %s. Debe estar entre 1 y 65535.\n", argv[2]);
            return EXIT_FAILURE;
        }
    }

    printf("================================================================\n");
    printf("   PUBLISHER TCP - TRANSMISION DE NOTICIAS DEPORTIVAS EN VIVO   \n");
    printf("================================================================\n");
    printf(" Destino Broker -> IP: %s | Puerto: %d\n", broker_ip, broker_port);
    printf("Total de eventos a transmitir: %d\n\n", total_eventos);

    /**
    creación del Socket TCP 
     */
    client_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (client_socket < 0) {
        perror(" Fallo en la creacion del socket");
        return EXIT_FAILURE;
    }
    printf(" Socket creado exitosamente (descriptor: %d).\n", client_socket);

    /**
     * configuración de la estructura sockaddr_in del Broker
     */
    memset(&broker_addr, 0, sizeof(broker_addr));
    broker_addr.sin_family = AF_INET;
    broker_addr.sin_port = htons(broker_port);

    if (inet_pton(AF_INET, broker_ip, &broker_addr.sin_addr) <= 0) {
        fprintf(stderr, " Direccion IP invalida o no soportada: %s\n", broker_ip);
        close(client_socket);
        return EXIT_FAILURE;
    }

    /**
     * establecimiento de la conexión mediante connect() (3 Way Handshake)
     */
    printf(" Conectando al Broker en %s:%d (Iniciando 3 Way Handshake)\n", broker_ip, broker_port);
    if (connect(client_socket, (struct sockaddr *)&broker_addr, sizeof(broker_addr)) < 0) {
        perror(" Fallo la conexion con el Broker");
        close(client_socket);
        return EXIT_FAILURE;
    }
    printf("Conexión TCP establecida con exito con el Broker.\n\n");

    /**
     * transmision secuencial de las incidencias del partido
     */
    for (int i = 0; i < total_eventos; i++) {
        /* formateo el mensaje incluyendo salto de linea delimitador para protocolos de flujo */
        snprintf(buffer, sizeof(buffer), "%s\n", cronograma_partido[i].evento);
        size_t longitud_mensaje = strlen(buffer);

        ssize_t bytes_enviados = send(client_socket, buffer, longitud_mensaje, 0);
        if (bytes_enviados < 0) {
            perror("Fallo en el envio del mensaje a traves del socker");
            break;
        }

        printf("[ENVIO %02d/%02d] (%zd bytes) -> %s", 
               i + 1, total_eventos, bytes_enviados, buffer);
        fflush(stdout);

        /* pausa entre mensajes para facilitar el analisis del trafico en wireshark */
        sleep(DELAY_SECONDS);
    }

    /**
     *  cierre ordenado de la conexión TCP mediante close() 
     */
    printf("\nFinalizo la transmisión de incidencias. Cerrando socket\n");
    if (close(client_socket) < 0) {
        perror(" Error al cerrar el socket");
        return EXIT_FAILURE;
    }

    printf("Socket cerrado limpiamente. Sesión TCP finalizada.\n");
    return EXIT_SUCCESS;
}
