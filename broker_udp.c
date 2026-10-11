#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>

#define PORT 8081
#define MAX_SUBSCRIBERS 100
#define BUFFER_SIZE 65535

int main(void) {
    int sock;
    struct sockaddr_in server_addr, client_addr;
    struct sockaddr_in subscribers[MAX_SUBSCRIBERS];
    socklen_t client_len;
    int count = 0;
    char buffer[BUFFER_SIZE];

    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        perror("socket");
        return EXIT_FAILURE;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(PORT);

    if (bind(sock, (struct sockaddr *)&server_addr,
             sizeof(server_addr)) < 0) {
        perror("bind");
        close(sock);
        return EXIT_FAILURE;
    }

    printf("[BROKER UDP] Escuchando en el puerto %d\n", PORT);
    fflush(stdout);

    while (1) {
        client_len = sizeof(client_addr);

        ssize_t n = recvfrom(sock, buffer, sizeof(buffer) - 1, 0,
            (struct sockaddr *)&client_addr, &client_len);

        if (n < 0) {
            perror("recvfrom");
            continue;
        }

        buffer[n] = '\0';

        if (strncmp(buffer, "SUBSCRIBE", 9) == 0) {
            int found = 0;

            for (int i = 0; i < count; i++) {
                if (subscribers[i].sin_addr.s_addr ==
                        client_addr.sin_addr.s_addr &&
                    subscribers[i].sin_port == client_addr.sin_port) {
                    found = 1;
                    break;
                }
            }

            if (!found && count < MAX_SUBSCRIBERS) {
                subscribers[count++] = client_addr;
                printf("[UDP] Suscriptor registrado: %s:%u\n",
                    inet_ntoa(client_addr.sin_addr),
                    (unsigned)ntohs(client_addr.sin_port));
            }
        } else {
            printf("[UDP] Noticia recibida: %s\n", buffer);

            for (int i = 0; i < count; i++) {
                ssize_t sent = sendto(sock, buffer, (size_t)n, 0,
                    (struct sockaddr *)&subscribers[i],
                    sizeof(subscribers[i]));

                if (sent < 0)
                    perror("sendto");
            }
        }

        fflush(stdout);
    }

    close(sock);
    return EXIT_SUCCESS;
}
