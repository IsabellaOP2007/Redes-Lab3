#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <netinet/in.h>

#define PORT 8080
#define MAX_CLIENTS FD_SETSIZE
#define BUFFER_SIZE 4096

int main(void) {
    int server_fd, client_fd;
    int clients[MAX_CLIENTS];
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);
    char buffer[BUFFER_SIZE];

    fd_set master, readfds;
    int max_fd;

    for (int i = 0; i < MAX_CLIENTS; i++)
        clients[i] = -1;

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        return EXIT_FAILURE;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR,
               &opt, sizeof(opt));

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&addr,
             sizeof(addr)) < 0) {
        perror("bind");
        close(server_fd);
        return EXIT_FAILURE;
    }

    if (listen(server_fd, 20) < 0) {
        perror("listen");
        close(server_fd);
        return EXIT_FAILURE;
    }

    FD_ZERO(&master);
    FD_SET(server_fd, &master);
    max_fd = server_fd;

    printf("[BROKER TCP] Escuchando en el puerto %d\n", PORT);
    printf("[BROKER TCP] Esperando publishers y subscribers...\n");
    fflush(stdout);

    while (1) {
        readfds = master;

        if (select(max_fd + 1, &readfds, NULL, NULL, NULL) < 0) {
            if (errno == EINTR)
                continue;
            perror("select");
            break;
        }

        for (int fd = 0; fd <= max_fd; fd++) {
            if (!FD_ISSET(fd, &readfds))
                continue;

            if (fd == server_fd) {
                client_fd = accept(server_fd,
                    (struct sockaddr *)&addr, &addr_len);

                if (client_fd < 0) {
                    perror("accept");
                    continue;
                }

                if (client_fd >= MAX_CLIENTS) {
                    close(client_fd);
                    continue;
                }

                int slot = -1;
                for (int i = 0; i < MAX_CLIENTS; i++) {
                    if (clients[i] == -1) {
                        slot = i;
                        break;
                    }
                }

                if (slot == -1) {
                    close(client_fd);
                    continue;
                }

                clients[slot] = client_fd;
                FD_SET(client_fd, &master);

                if (client_fd > max_fd)
                    max_fd = client_fd;

                printf("[TCP] Cliente conectado: descriptor %d\n",
                       client_fd);
                fflush(stdout);
            } else {
                ssize_t n = recv(fd, buffer, sizeof(buffer), 0);

                if (n <= 0) {
                    printf("[TCP] Cliente desconectado: %d\n", fd);
                    close(fd);
                    FD_CLR(fd, &master);

                    for (int i = 0; i < MAX_CLIENTS; i++) {
                        if (clients[i] == fd) {
                            clients[i] = -1;
                            break;
                        }
                    }
                    continue;
                }

                printf("[TCP] Recibidos %zd bytes del cliente %d\n",
                       n, fd);

                for (int i = 0; i < MAX_CLIENTS; i++) {
                    int dest = clients[i];

                    if (dest == -1 || dest == fd)
                        continue;

                    size_t sent = 0;

                    while (sent < (size_t)n) {
                        ssize_t r = send(dest, buffer + sent,
                                         (size_t)n - sent,
                                         MSG_NOSIGNAL);

                        if (r <= 0)
                            break;

                        sent += (size_t)r;
                    }
                }
                fflush(stdout);
            }
        }
    }

    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i] != -1)
            close(clients[i]);
    }

    close(server_fd);
    return EXIT_SUCCESS;
}
