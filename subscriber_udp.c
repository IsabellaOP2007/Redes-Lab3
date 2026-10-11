/*
 * Suscriptor UDP para el sistema Pub-Sub.
 * Uso: ./subscriber_udp PUERTO_LOCAL
 *
 * Se registra en el broker enviando "SUBSCRIBE"
 * y después recibe las noticias deportivas.
 */

#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define BROKER_IP "127.0.0.1"
#define BROKER_PORT 8081
#define BUFFER_SIZE 65535

int main(int argc, char **argv) {

    /* Validar argumentos */
    if (argc != 2) {
        fprintf(stderr, "Uso: %s PUERTO_LOCAL\n", argv[0]);
        return 1;
    }

    char *end;
    errno = 0;
    long port = strtol(argv[1], &end, 10);

    if (errno || end == argv[1] || *end ||
        port < 1 || port > 65535) {
        fprintf(stderr, "Puerto invalido\n");
        return 1;
    }

    /* Crear socket UDP */
    int fd = socket(AF_INET, SOCK_DGRAM, 0);

    if (fd < 0) {
        perror("Error creando socket");
        return 1;
    }

    /* Configurar direccion local */
    struct sockaddr_in local = {0};
    local.sin_family = AF_INET;
    local.sin_addr.s_addr = htonl(INADDR_ANY);
    local.sin_port = htons((unsigned short)port);

    /* Asociar socket al puerto local */
    if (bind(fd, (struct sockaddr *)&local, sizeof(local)) < 0) {
        perror("Error en bind");
        close(fd);
        return 1;
    }

    /* Configurar direccion del broker */
    struct sockaddr_in broker = {0};
    broker.sin_family = AF_INET;
    broker.sin_port = htons(BROKER_PORT);

    if (inet_pton(AF_INET, BROKER_IP, &broker.sin_addr) != 1) {
        fprintf(stderr, "Error con la IP del broker\n");
        close(fd);
        return 1;
    }

    /* Registrar suscripcion en el broker */
    const char *subscription = "SUBSCRIBE";

    if (sendto(fd, subscription, strlen(subscription), 0,
               (struct sockaddr *)&broker, sizeof(broker)) < 0) {
        perror("Error registrando suscripcion");
        close(fd);
        return 1;
    }

    printf("Suscriptor UDP iniciado.\n");
    printf("Puerto local: %ld\n", port);
    printf("Broker: %s:%d\n", BROKER_IP, BROKER_PORT);
    printf("Suscripcion registrada. Esperando noticias...\n\n");

    /* Recibir noticias */
    char buffer[BUFFER_SIZE];

    while (1) {
        ssize_t n = recvfrom(fd, buffer, sizeof(buffer), 0,
                             NULL, NULL);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }

            perror("Error recibiendo noticias");
            close(fd);
            return 1;
        }

        printf("Noticia recibida: ");

        fwrite(buffer, 1, (size_t)n, stdout);

        if (n == 0 || buffer[n - 1] != '\n') {
            putchar('\n');
        }

        fflush(stdout);
    }

    close(fd);
    return 0;
}
