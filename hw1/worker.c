#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <fcntl.h>              

#define BROADCAST_PORT      8888
#define REQUEST_HELLO       0
#define REQUEST_SEND_TASK   1
#define REQUEST_PING        2
#define RESPONSE_PING       2
#define RESPONSE_TASK_READY 3
#define RESPONSE_HELLO      0
#define MAX_WORKERS         10

typedef struct {
    double startX;
    double endX;
} TaskArguments;

typedef struct {
    int type;
    TaskArguments taskAgrs;
} Request;

typedef struct {
    int type;
    short helloWorkerPort;
    double taskReadyResult;
} Response;


double function(double x) {
    return x * x;
}

double solve_task(double startX, double endX) {
    // Вычисление интеграла методом прямоугольников
    double h = (endX - startX) / MAX_WORKERS;
    double sum = 0.0;
    for (int i = 0; i < MAX_WORKERS; i++) {
            double x = startX + h * (i + 0.5);
            sum += function(x);
    }
    double result = sum * h;
    return result;
}

void listen_for_master_hello(int port, int id) {
    int master_socket;
    struct sockaddr_in master_addr;

    if ((master_socket = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        perror("UDP socket creation failed\n");
        exit(EXIT_FAILURE);
    }

    //неблокирующий сокет
    int flags = fcntl(master_socket, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl get failed\n");
        close(master_socket);
        exit(EXIT_FAILURE);
    }
    if (fcntl(master_socket, F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("fcntl set failed\n");
        close(master_socket);
        exit(EXIT_FAILURE);
    }

    memset(&master_addr, 0, sizeof(master_addr));
    master_addr.sin_family = AF_INET;
    master_addr.sin_addr.s_addr = INADDR_ANY;
    master_addr.sin_port = htons(BROADCAST_PORT + id);

    if (bind(master_socket, (struct sockaddr *)&master_addr, sizeof(master_addr)) < 0) {
        perror("error binding UDP socket");
        close(master_socket);
        exit(EXIT_FAILURE);
    }

    while (1) {
        Request request;
        Response response;

        socklen_t master_addr_len = sizeof(master_addr);
        size_t bytes_received = recvfrom(master_socket, &request, sizeof(request), 0, (struct sockaddr *)&master_addr, &master_addr_len);
        if (bytes_received == sizeof(request)) {
            if (request.type == REQUEST_HELLO) {
                response.type = RESPONSE_HELLO;
                response.helloWorkerPort = port;

                if (sendto(master_socket, &response, sizeof(response), 0, (struct sockaddr *)&master_addr,  sizeof(master_addr)) < 0) {
                    continue;
                }

                printf("got hello from master\n");

                break;
            }
        }
    }

    close(master_socket);
}

void listen_for_master_client_connection(int port) {
    int worker_socket, master_socket = -1;
    struct sockaddr_in worker_addr;
    
    if ((worker_socket = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("TCP socket creation failed");
        exit(EXIT_FAILURE);
    }

    worker_addr.sin_family = AF_INET;
    worker_addr.sin_addr.s_addr = INADDR_ANY;
    worker_addr.sin_port = htons(port);

    if (bind(worker_socket, (struct sockaddr *)&worker_addr, sizeof(worker_addr)) == -1) {
        perror("error binding tcp socket");
        close(worker_socket);
        exit(EXIT_FAILURE);
    }

    if (listen(worker_socket, 5) == -1) {
        perror("error listening tcp socket");
        close(worker_socket);
        exit(EXIT_FAILURE);
    }

    int flags = fcntl(worker_socket, F_GETFL, 0);
    fcntl(worker_socket, F_SETFL, flags | O_NONBLOCK);

    fd_set readfds;

    while (1) {
        FD_ZERO(&readfds);
        FD_SET(worker_socket, &readfds);

        struct timeval timeout;
        timeout.tv_sec = 3;
        timeout.tv_usec = 0;

        int activity = select(worker_socket + 1, &readfds, NULL, NULL, &timeout);
        
        if (activity < 0) {
            perror("select error");
            continue;
        } else if (activity == 0) {
            continue;
        }

        if (FD_ISSET(worker_socket, &readfds)) {
            socklen_t worker_addr_len = sizeof(worker_addr);
            if ((master_socket = accept(worker_socket, (struct sockaddr *)&worker_addr, &worker_addr_len)) == -1) {
                perror("accept failed");
                continue;
            }

            Request request;
            Response response;
            ssize_t bytes_read = recv(master_socket, &request, sizeof(request), 0);
            if (bytes_read == sizeof(request)) {
                if (request.type == REQUEST_PING) {
                    response.type = RESPONSE_PING;
                    send(master_socket, &response, sizeof(response), 0);
                } else if (request.type == REQUEST_SEND_TASK) {
                    response.taskReadyResult = solve_task(request.taskAgrs.startX, request.taskAgrs.endX);
                    response.type = RESPONSE_TASK_READY;
                    send(master_socket, &response, sizeof(response), 0);
                }
            }

            close(master_socket);
        }
    }

    close(worker_socket);
}

int main(int argc, char* argv[]){
    int port = atoi(argv[1]);
    int id = atoi(argv[2]);
    
    listen_for_master_hello(port, id);
    
    listen_for_master_client_connection(port);

    return 0;
}
