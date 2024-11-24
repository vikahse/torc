#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>             
#include <arpa/inet.h>          
#include <sys/socket.h>
#include <fcntl.h>              
#include <pthread.h>            
#include <errno.h>
#include <stdbool.h>
#include <time.h>
#include <pthread.h>

#define BROADCAST_PORT              8888
#define MAX_WORKERS                 10
#define WORKER_STATUS_READY         0 
#define WORKER_STATUS_DISCONNECTED -1
#define WORKER_STATUS_BUSY          1
#define TASK_STATUS_READY_TO_RUN    0
#define TASK_STATUS_IN_PROGRESS     1
#define TASK_STATUS_DONE            2
#define REQUEST_HELLO               0
#define REQUEST_SEND_TASK           1
#define REQUEST_PING                2
#define RESPONSE_PING               2
#define RESPONSE_TASK_READY         3       
#define RESPONSE_HELLO              0
#define TIME_OUT_OF_WAITING         4
#define TIME_OUT_OF_PING            3
#define PING_STATE_WAIT             0
#define PING_STATE_OK               1

typedef struct {
    double startX;
    double endX;
} TaskArguments;

typedef struct {
    int status;
    TaskArguments args;
    double result;
    int workerID;
} Task;

typedef struct {
    struct sockaddr_in addr;
    short port;
    int status; 
    int taskID;
} WorkerNode;

typedef struct {
    int type;
    TaskArguments taskAgrs;
} Request;

typedef struct {
    int type;
    short helloWorkerPort;
    double taskReadyResult;
} Response;

WorkerNode workers[MAX_WORKERS];
Task tasks[MAX_WORKERS];
pthread_t threads[MAX_WORKERS];
int workers_count = 0;
int ready_tasks_count = 0;
pthread_mutex_t mutex;

void init_tasks_pool(double globalStartX, double globalEndX);
int send_hello();
void add_worker_to_pool(struct sockaddr_in addr, short port);
bool run_tasks();
void find_free_task_and_worker(bool *free_task, bool *free_worker, bool *busy_worker, int *taskID, int *workerID);
void run_worker_with_task(int workerID, int taskID);
void* worker_thread_func(void* arg);
void set_worker_disconnected(int workerID, int taskID);
void set_worker_ready(int workerID, int taskID);
int get_answer(int worker_socket, double* outTaskResult);
double get_final_result();
void* worker_status_monitor(void* arg);
bool ping_worker(WorkerNode* worker);

//    sudo tc qdisc add dev eth0 root netem loss 100% - полная потеря
//    sudo tc qdisc add dev eth0 root netem loss 10% - частияная потеря 
//    sudo tc qdisc add dev eth0 root netem duplicate 5% - дублированием
//    sudo tc qdisc add dev eth0 root netem delay 100ms - задержки 
//    sudo tc qdisc add dev eth0 root netem delay 100ms loss 10% duplicate 5% - сочетание условий
//    sudo iptables -A INPUT -p udp --dport 8888 -m statistic --mode random --probability 0.1 -j DROP - потеря частичная пакетов

//  - init tasks in pool
//  - send hello - collect "ready" servers
//      collect pool of Workers and Tasks
//      init Workers in pool
//  - connect to Workers
//  - run tasks
//  - get final result from workers
int main() {
    if (pthread_mutex_init(&mutex, NULL) != 0) {
        perror("mutex init failed\n");
        exit(EXIT_FAILURE);
    }

    init_tasks_pool(0.0, 10.0);    
    
    if (send_hello() <= 0) {
        perror("no workers were found\n");
        exit(EXIT_FAILURE);
    }

    printf("COUNT OF WORKERS: %d\n", workers_count);

    bool result = run_tasks();
    if (!result) {
        perror("cannot finish all tasks because all workers are disconnected\n");
        exit(EXIT_FAILURE);
    }

    double final_result = get_final_result();
    printf("ANSWER: %f\n", final_result);

    pthread_mutex_destroy(&mutex);
}

// init tasks in pool
void init_tasks_pool(double globalStartX, double globalEndX)
{   
    double segmentLength = (globalEndX - globalStartX) / MAX_WORKERS;
    double previousEndX = globalStartX;
    for (int i = 0; i < MAX_WORKERS; i++) {
        tasks[i].status = TASK_STATUS_READY_TO_RUN;
        tasks[i].workerID = -1;

        TaskArguments taskArgs;
        taskArgs.startX = previousEndX;
        previousEndX = (i == MAX_WORKERS - 1) ? globalEndX : taskArgs.startX + segmentLength; 
        taskArgs.endX = previousEndX;
        tasks[i].args = taskArgs;
    }
}

// collect ready servers
int send_hello() {
    int sockfd;
    struct sockaddr_in servaddr;

    workers_count = 0;

    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) { //сокет с использованием IPv4 и протокола UDP
        perror("UDP master socket creation failed\n");
        exit(EXIT_FAILURE);
    }

    //https://www.rsdn.org/article/unix/sockets.xml
    //неблокирующий сокет
    int flags = fcntl(sockfd, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl get failed\n");
        close(sockfd);
        exit(EXIT_FAILURE);
    }
    if (fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("fcntl set failed\n");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    int broadcastPermission = 1;
    if (setsockopt(sockfd, SOL_SOCKET,  SO_BROADCAST, (char*)&broadcastPermission, sizeof(broadcastPermission)) == -1) {
        perror("setsockopt for SO_BROADCAST failed\n");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = INADDR_ANY;
    // inet_pton(AF_INET, "255.255.255.255", &servaddr.sin_addr);

    Request request;
    request.type = REQUEST_HELLO;
    for(int i = 0; i < MAX_WORKERS; i++) {
        servaddr.sin_port = htons(BROADCAST_PORT + i);

        if (sendto(sockfd, &request, sizeof(request), 0, (struct sockaddr *)&servaddr,  sizeof(servaddr)) == -1) {
            perror("sendto failed");
            close(sockfd);
            exit(EXIT_FAILURE);
        }
    }

    time_t time_start;
    time(&time_start);
    while (1) {
        time_t time_cur;
        time(&time_cur);
        if(time_cur - time_start > TIME_OUT_OF_WAITING) {
            break;
        }
        pthread_mutex_lock(&mutex);
        if (workers_count > MAX_WORKERS) {
            pthread_mutex_unlock(&mutex);
            break;
        }
        pthread_mutex_unlock(&mutex);

        struct sockaddr_in cliaddr;
        socklen_t cliaddr_len = sizeof(cliaddr);
        Response response;
        int n_bytes = recvfrom(sockfd, &response, sizeof(response), 0, (struct sockaddr *)&cliaddr, &cliaddr_len);
        if (n_bytes == sizeof(response)) {
            if(response.type == RESPONSE_HELLO){
                add_worker_to_pool(cliaddr, response.helloWorkerPort);
            }
        }

        usleep(500 * 1000); // задержка на 500000 микросекунд = 500 миллисекунд
    }

    close(sockfd);
    return workers_count;
}

// init workers in pool
void add_worker_to_pool(struct sockaddr_in addr, short port)
{   
    pthread_mutex_lock(&mutex);
    if (workers_count >= MAX_WORKERS) {
        pthread_mutex_unlock(&mutex);
        return;
    }
    int i = workers_count;
    workers[i].addr = addr;
    workers[i].port = port;
    workers[i].status = WORKER_STATUS_READY;
    workers[i].taskID = -1;
    workers_count++;
    pthread_mutex_unlock(&mutex);
}

bool run_tasks()
{   
    pthread_t monitor_thread;
    if (pthread_create(&monitor_thread, NULL, worker_status_monitor, NULL) != 0) {
        perror("failed to create monitor thread");
        return false;
    }
    pthread_detach(monitor_thread);

    while (1) {
        bool free_task = false;
        bool free_worker = false;
        bool busy_worker = false;
        int taskID = -1;
        int workerID = -1;

        find_free_task_and_worker(&free_task, &free_worker, &busy_worker, &taskID, &workerID);
        if (!free_task) { //если нет свободных задач, заканчиваем работу
            return true;
        }
        if (free_task && !free_worker && !busy_worker) { //если есть свободные задачи, но нет свободных воркеров и нет ни одного в процессе, то заканчиваем с ошибкой 
            return false;
        } else if (free_task && !free_worker && busy_worker) { //если есть свободные задачи, нет свободных воркеров, но есть воркеры, которые работают, то ждем их
            usleep(900 * 1000);
            continue;
        }

        run_worker_with_task(workerID, taskID);

        usleep(200 * 1000);
    }
}

void find_free_task_and_worker(bool *free_task, bool *free_worker, bool *busy_worker, int *taskID, int *workerID) {
    int taskStatus;
    int workerStatus;
    for (int i = 0; i < MAX_WORKERS; i++) {
        pthread_mutex_lock(&mutex);
        taskStatus = tasks[i].status;
        pthread_mutex_unlock(&mutex);
        if (taskStatus == TASK_STATUS_READY_TO_RUN) {
            *free_task = true;
            *taskID = i;
            break;
        }
    }

    if (!(*free_task)) {
        return;
    }

    for (int i = 0; i < workers_count; i++) {
        pthread_mutex_lock(&mutex);
            workerStatus = workers[i].status;
        pthread_mutex_unlock(&mutex);
        if (workerStatus == WORKER_STATUS_READY) {
            *free_worker = true;
            *workerID = i;
            break;
        } else if (workerStatus == WORKER_STATUS_BUSY) {
            *busy_worker = true;
        }
    }
}

void run_worker_with_task(int workerID, int taskID)
{   
    pthread_mutex_lock(&mutex);
        int taskStatus = tasks[taskID].status;    

    if (taskID == -1) 
        return;
    if (taskStatus != TASK_STATUS_READY_TO_RUN)
        return;

        workers[workerID].status = WORKER_STATUS_BUSY;
        workers[workerID].taskID = taskID;

        tasks[taskID].status = TASK_STATUS_IN_PROGRESS;
        tasks[taskID].workerID = workerID;
    pthread_mutex_unlock(&mutex);

    pthread_t tid;    
    int result = pthread_create(&tid, NULL, worker_thread_func, &workerID);
    if(result != 0)
    {
        perror("unable to run new thread\n");
        exit(EXIT_FAILURE);
    }
    // pthread_detach(tid);
    pthread_join(tid, NULL);
}

void* worker_thread_func(void* arg)
{   
    int workerID = *(int*)arg; 
    WorkerNode *worker = &workers[workerID];
    int taskID = worker->taskID;
    Task *task = &tasks[taskID];

    int worker_socket = socket(AF_INET, SOCK_STREAM, 0); //TCP
    if (worker_socket < 0){
        perror("socket creation failed\n");
        exit(EXIT_FAILURE);
    }

    //https://www.rsdn.org/article/unix/sockets.xml
    //неблокирующий сокет
    int flags = fcntl(worker_socket, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl get failed\n");
        close(worker_socket);
        exit(EXIT_FAILURE);
    }
    if (fcntl(worker_socket, F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("fcntl set failed\n");
        close(worker_socket);
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in worker_addr;
    memset(&worker_addr, 0, sizeof(worker_addr));
    worker_addr.sin_family = AF_INET;
    worker_addr.sin_port = htons(worker->port);
    worker_addr.sin_addr = worker->addr.sin_addr;

    if (connect(worker_socket, (struct sockaddr *)&worker_addr, sizeof(worker_addr)) < 0) {
        if (errno != EINPROGRESS){
            perror("connection failed\n");
            close(worker_socket);
            exit(EXIT_FAILURE);
        }
    }

    Request request;
    request.type = REQUEST_SEND_TASK;
    request.taskAgrs.endX = task->args.endX;
    request.taskAgrs.startX = task->args.startX;
    if (send(worker_socket, &request, sizeof(request), 0) == -1) {
        set_worker_ready(workerID, taskID);
        close(worker_socket);
        return 0;
    }

    double taskResult;
    int result = get_answer(worker_socket, &taskResult); 
    
    // DISCONNECTED будет для тех кто совсем умер, иначе можем перевести в READY
    // Виды ошибок:
    // - ошибка bind (критичная) -> WORKER_STATUS_DISCONNECTED
    // - ошибка socket (критичная) -> WORKER_STATUS_DISCONNECTED
    // - ошибка sendto (могут быть проблемы с сетью) -> WORKER_STATUS_READY (будем пробовать опять соединиться)
    // - задача решена (успех) -> WORKER_STATUS_READY
    // - таймаут на ожидание пинга истёк -> WORKER_STATUS_DISCONNECTED (воркер умер) (продолжаем пинговать -> если ответил, то переводим в WORKER_STATUS_READY)

    if (result == 0) {
        pthread_mutex_lock(&mutex);
            worker->status = WORKER_STATUS_READY;
            worker->taskID = -1;
            task->status = TASK_STATUS_DONE;
            task->workerID = workerID;
            task->result = taskResult;
        pthread_mutex_unlock(&mutex);
    } else if (result == 1) {
        set_worker_ready(workerID, taskID);
    } else if (result == 2) { // если воркер не проснулся в течение таймаута, то считаем его погибшим (будем продолжать в отдельном потоке пинговать всех отключившихся)
        set_worker_disconnected(workerID, taskID);
    }

    close(worker_socket);

    return 0;   
}

void set_worker_disconnected(int workerID, int taskID) {
    pthread_mutex_lock(&mutex);
        workers[workerID].status = WORKER_STATUS_DISCONNECTED;
        workers[workerID].taskID = -1;
        tasks[taskID].status = TASK_STATUS_READY_TO_RUN;
        tasks[taskID].workerID = -1;
    pthread_mutex_unlock(&mutex);
}

void set_worker_ready(int workerID, int taskID) {
    pthread_mutex_lock(&mutex);
        workers[workerID].status = WORKER_STATUS_READY;
        workers[workerID].taskID = -1;
        tasks[taskID].status = TASK_STATUS_READY_TO_RUN;
        tasks[taskID].workerID = -1;
    pthread_mutex_unlock(&mutex);
}

int get_answer(int worker_socket, double* outTaskResult)
{
    time_t ping_time_start = 0;
    int ping_state = PING_STATE_OK;
    
    while (1) {
        time_t time_cur;
        time(&time_cur);
        
        //  таймаут истёк!
        if (ping_state == PING_STATE_WAIT && time_cur - ping_time_start > TIME_OUT_OF_PING) {
            return 2;        
        }

        Response response;
        int n_bytes = recv(worker_socket, &response, sizeof(response), 0);
        if (n_bytes == sizeof(response)) 
        {
            if (response.type == RESPONSE_PING) {
                ping_state = PING_STATE_OK;
            } else if (response.type == RESPONSE_TASK_READY) 
            {
                *outTaskResult = response.taskReadyResult;
                return 0;
            }
        }

        //  send ping
        time(&time_cur);
        if (ping_state == PING_STATE_OK && time_cur - ping_time_start >= TIME_OUT_OF_PING) {
            time(&ping_time_start);
            ping_state = PING_STATE_WAIT;

            Request request;
            request.type = REQUEST_PING;
            if (send(worker_socket, &request, sizeof(request), 0) == -1) {
                return 1;
            }
        }

        usleep(500 * 1000);
    }
}

// sum all results from workers
double get_final_result() {
    double answer = 0;
    for (int i = 0; i < MAX_WORKERS; i++) {
        answer += tasks[i].result;
    }
    return answer;
}

void* worker_status_monitor(void* arg) {
    while (1) {
        for (int i = 0; i < workers_count; i++) {
            pthread_mutex_lock(&mutex);
            WorkerNode* worker = &workers[i];
            if (worker->status == WORKER_STATUS_DISCONNECTED) {
                if (ping_worker(worker)) {
                    worker->status = WORKER_STATUS_READY;
                }
            }
            pthread_mutex_unlock(&mutex);
        }
        usleep(1000 * 1000);
    }
}

bool ping_worker(WorkerNode* worker) {
    int worker_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (worker_socket < 0) {
        return false; 
    }

    struct sockaddr_in worker_addr;
    worker_addr.sin_family = AF_INET;
    worker_addr.sin_port = worker->port;
    worker_addr.sin_addr = worker->addr.sin_addr;

    // установка таймаута для recv
    struct timeval timeout;
    timeout.tv_sec = 0;
    timeout.tv_usec = 500000; //500 миллисекунд
    setsockopt(worker_socket, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout));

    if (connect(worker_socket, (struct sockaddr*)&worker_addr, sizeof(worker_addr)) < 0) {
        close(worker_socket);
        return false;
    }

    Request ping_request;
    ping_request.type = REQUEST_PING;
    if (send(worker_socket, &ping_request, sizeof(ping_request), 0) < 0) {
        close(worker_socket);
        return false;
    }
    ping_request.type = REQUEST_HELLO;
    if (send(worker_socket, &ping_request, sizeof(ping_request), 0) < 0) {
        close(worker_socket);
        return false;
    }

    Response response;
    int n_bytes = recv(worker_socket, &response, sizeof(response), 0);
    if (n_bytes == sizeof(response))  {
        close(worker_socket);
        return true;
    }

    close(worker_socket);
    return false;
}
