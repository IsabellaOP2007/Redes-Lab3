/* Recepción preliminar: el registro de suscripción depende del broker del grupo.
 * Uso: subscriber_tcp IP PUERTO. Noticias delimitadas por '\n'. */
#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "Uso: %s IP PUERTO\n", argv[0]);
        return 1;
    }
    char *end;
    errno = 0;
    long port = strtol(argv[2], &end, 10);
    if (errno || end == argv[2] || *end || port < 1 || port > 65535) {
        fprintf(stderr, "Puerto inválido\n");
        return 1;
    }
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((unsigned short)port);
    if (inet_pton(AF_INET, argv[1], &addr.sin_addr) != 1) {
        fprintf(stderr, "Se requiere una dirección IPv4 válida\n");
        return 1;
    }
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect"); close(fd); return 1;
    }
    fprintf(stderr, "Conectado. Recepción preliminar sin registro de suscripción.\n");
    char chunk[1024], line[8192];
    size_t used = 0;
    int status = 0;
    for (;;) {
        ssize_t n = recv(fd, chunk, sizeof(chunk), 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("recv"); status = 1; break;
        }
        if (n == 0) {
            if (used) {
                fprintf(stderr, "El servidor cerró con una noticia incompleta\n");
                status = 1;
            }
            break;
        }
        for (ssize_t i = 0; i < n; ++i) {
            if (chunk[i] == '\n') {
                fwrite(line, 1, used, stdout);
                putchar('\n'); fflush(stdout); used = 0;
            } else {
                if (used == sizeof(line)) {
                    fprintf(stderr, "Noticia excede el límite de 8192 bytes\n");
                    close(fd); return 1;
                }
                line[used++] = chunk[i];
            }
        }
    }
    close(fd);
    return status;
}
