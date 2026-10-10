/* Receptor UDP preliminar. Uso: subscriber_udp PUERTO_LOCAL.
 * No registra una suscripción: falta acordar el protocolo con el broker. */
#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "Uso: %s PUERTO_LOCAL\n", argv[0]); return 1;
    }
    char *end;
    errno = 0;
    long port = strtol(argv[1], &end, 10);
    if (errno || end == argv[1] || *end || port < 1 || port > 65535) {
        fprintf(stderr, "Puerto inválido\n"); return 1;
    }
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) { perror("socket"); return 1; }
    struct sockaddr_in local = {0};
    local.sin_family = AF_INET;
    local.sin_addr.s_addr = htonl(INADDR_ANY);
    local.sin_port = htons((unsigned short)port);
    if (bind(fd, (struct sockaddr *)&local, sizeof(local)) < 0) {
        perror("bind"); close(fd); return 1;
    }
    fprintf(stderr, "Escuchando UDP en %ld. Sin registro de suscripción. Ctrl+C para salir.\n", port);
    /* Capacidad suficiente para cualquier payload de un datagrama UDP IPv4. */
    char buffer[65535];
    for (;;) {
        ssize_t n = recvfrom(fd, buffer, sizeof(buffer), 0, NULL, NULL);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("recvfrom"); close(fd); return 1;
        }
        fwrite(buffer, 1, (size_t)n, stdout);
        if (n == 0 || buffer[n - 1] != '\n') putchar('\n');
        fflush(stdout);
    }
}
