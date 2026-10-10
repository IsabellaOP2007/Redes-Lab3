/* PRUEBA AUXILIAR EN C — no es el broker ni registra suscripciones.
 * Ejecutar desde la raíz del repositorio:
 * mkdir -p build
 * cc -std=c11 -Wall -Wextra -Werror subscriber_tcp.c -o build/subscriber_tcp
 * cc -std=c11 -Wall -Wextra -Werror subscriber_udp.c -o build/subscriber_udp
 * cc -std=c11 -Wall -Wextra -Werror test_subscribers.c -o build/test_subscribers
 * ./build/test_subscribers
 *
 * Usa sockets y procesos POSIX disponibles en macOS/Linux, sin librerías externas.
 * Comprueba recepción en loopback, no entrega final del sistema Pub/Sub.
 * Pendiente acordar con el grupo puertos, identificación de partidos y formato
 * de suscripción/confirmación; luego probar con 1 broker, 2 publishers y
 * 2 subscribers y guardar capturas Wireshark y resultados para la tabla.
 */
#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

static void check(int ok, const char *what) {
    if (!ok) { fprintf(stderr, "FALLO: %s\n", what); exit(1); }
}

static int endpoint(int type, char port[6]) {
    int fd = socket(AF_INET, type, 0);
    check(fd >= 0, "socket de prueba");
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    check(inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) == 1, "IPv4 loopback");
    check(bind(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0, "bind de prueba");
    socklen_t len = sizeof(addr);
    check(getsockname(fd, (struct sockaddr *)&addr, &len) == 0, "puerto efímero");
    snprintf(port, 6, "%u", ntohs(addr.sin_port));
    return fd;
}

static pid_t launch(const char *program, const char *port, int output, int errors) {
    pid_t pid = fork();
    check(pid >= 0, "fork");
    if (!pid) {
        if (dup2(output, STDOUT_FILENO) < 0 || dup2(errors, STDERR_FILENO) < 0) _exit(126);
        /* No heredar sockets ni extremos de pipes que impedirían detectar EOF. */
        long limit = sysconf(_SC_OPEN_MAX);
        if (limit < 0) limit = 1024;
        for (int fd = 3; fd < limit; ++fd) close(fd);
        if (strstr(program, "tcp")) execl(program, program, "127.0.0.1", port, (char *)NULL);
        else execl(program, program, port, (char *)NULL);
        _exit(127);
    }
    return pid;
}

static size_t read_all(int fd, char *buffer, size_t cap) {
    size_t used = 0;
    while (used < cap) {
        ssize_t n = read(fd, buffer + used, cap - used);
        if (n < 0 && errno == EINTR) continue;
        check(n >= 0, "lectura de salida");
        if (!n) break;
        used += (size_t)n;
    }
    close(fd);
    return used;
}

static void send_all(int fd, const char *data, size_t size) {
    while (size) {
        ssize_t n = send(fd, data, size, 0);
        if (n < 0 && errno == EINTR) continue;
        check(n > 0, "envío TCP de prueba");
        data += n; size -= (size_t)n;
    }
}

static void tcp_case(const char **parts, size_t count, const char *expected, int code) {
    char port[6];
    int server = endpoint(SOCK_STREAM, port), out[2], err[2];
    check(listen(server, 1) == 0, "listen");
    check(pipe(out) == 0 && pipe(err) == 0, "pipes");
    pid_t pid = launch("./build/subscriber_tcp", port, out[1], err[1]);
    close(out[1]); close(err[1]);
    int peer = accept(server, NULL, NULL);
    check(peer >= 0, "accept");
    for (size_t i = 0; i < count; ++i) send_all(peer, parts[i], strlen(parts[i]));
    close(peer); close(server);
    char result[16384], errors[1024];
    size_t n = read_all(out[0], result, sizeof(result));
    read_all(err[0], errors, sizeof(errors));
    int status;
    check(waitpid(pid, &status, 0) == pid, "waitpid TCP");
    check(WIFEXITED(status) && WEXITSTATUS(status) == code, "estado TCP");
    check(n == strlen(expected) && memcmp(result, expected, n) == 0, "contenido TCP");
}

static void bad_port(const char *program) {
    int out[2], err[2], status;
    check(pipe(out) == 0 && pipe(err) == 0, "pipes argumentos");
    pid_t pid = launch(program, "80abc", out[1], err[1]);
    close(out[1]); close(err[1]);
    char buffer[1024];
    read_all(out[0], buffer, sizeof(buffer));
    read_all(err[0], buffer, sizeof(buffer));
    check(waitpid(pid, &status, 0) == pid, "waitpid argumentos");
    check(WIFEXITED(status) && WEXITSTATUS(status) == 1, "rechazo de puerto inválido");
}

static void udp_case(void) {
    char port[6];
    int reserve = endpoint(SOCK_DGRAM, port), out[2], err[2];
    close(reserve); /* El subscriber toma el puerto; si está ocupado falla el test. */
    check(pipe(out) == 0 && pipe(err) == 0, "pipes UDP");
    pid_t pid = launch("./build/subscriber_udp", port, out[1], err[1]);
    close(out[1]); close(err[1]);
    FILE *errors = fdopen(err[0], "r");
    check(errors != NULL, "fdopen");
    char ready[256];
    int started = fgets(ready, sizeof(ready), errors) && strstr(ready, "Escuchando");
    if (!started) {
        kill(pid, SIGKILL); waitpid(pid, NULL, 0);
        check(0, "inicio UDP");
    }
    int sender = socket(AF_INET, SOCK_DGRAM, 0);
    check(sender >= 0, "socket emisor UDP");
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    check(inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) == 1, "IPv4 loopback");
    addr.sin_port = htons((unsigned short)strtol(port, NULL, 10));
    const char *parts[] = {"Gol uno\n", "Gol dos", ""};
    for (size_t i = 0; i < 3; ++i) {
        size_t size = strlen(parts[i]);
        check(sendto(sender, parts[i], size, 0, (struct sockaddr *)&addr, sizeof(addr)) == (ssize_t)size, "envío UDP");
    }
    close(sender);
    const char *expected = "Gol uno\nGol dos\n\n";
    char result[64];
    size_t used = 0, size = strlen(expected);
    while (used < size) {
        ssize_t n = read(out[0], result + used, size - used);
        if (n < 0 && errno == EINTR) continue;
        check(n > 0, "recepción UDP"); used += (size_t)n;
    }
    kill(pid, SIGTERM);
    check(waitpid(pid, NULL, 0) == pid, "waitpid UDP");
    close(out[0]); fclose(errors);
    check(memcmp(result, expected, size) == 0, "contenido UDP");
}

int main(void) {
    /* Tiempo máximo del test para evitar una espera indefinida. */
    alarm(15);
    const char *parts[] = {"Go", "l uno\nGol dos\nGo", "l tres\n"};
    tcp_case(parts, 3, "Gol uno\nGol dos\nGol tres\n", 0);
    const char *incomplete[] = {"incompleto"};
    tcp_case(incomplete, 1, "", 1);
    char large[8194]; memset(large, 'x', sizeof(large) - 1); large[8193] = '\0';
    const char *overflow[] = {large};
    tcp_case(overflow, 1, "", 1);
    bad_port("./build/subscriber_tcp");
    bad_port("./build/subscriber_udp");
    udp_case();
    puts("OK: recepción TCP, EOF incompleto, límite, puertos inválidos y 3 datagramas UDP.");
    return 0;
}
