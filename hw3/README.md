**Запуск:**

`python hw3/server.py`    

`PORT=8001 python hw3/server.py` 

**Регистрация:**

`curl -X POST http://localhost:8000/register -H "Content-Type: application/json" -d '{"url": "http://localhost:8001"}'`

`curl -X POST http://localhost:8001/register -H "Content-Type: application/json" -d '{"url": "http://localhost:8000"}'`

**Тест кейс 1:**

`curl -X PATCH http://localhost:8000/update -H "Content-Type: application/json" -d '{"updates": {"key1": "value1"}}'`

`curl -X GET http://localhost:8001/get/key1  -H "Content-Type: application/json"`

`curl -X GET http://localhost:8000/get/key1  -H "Content-Type: application/json"`

**Тест кейс 2:**

`curl -X PATCH http://localhost:8001/update -H "Content-Type: application/json" -d '{"updates": {"key2": "value2"}}'`

`curl -X GET http://localhost:8000/get/key2  -H "Content-Type: application/json"`

`curl -X GET http://localhost:8001/get/key2  -H "Content-Type: application/json"`

**Тест кейс 3: (update)**

`curl -X PATCH http://localhost:8001/update -H "Content-Type: application/json" -d '{"updates": {"key1": "value2"}}'`

`curl -X GET http://localhost:8000/get/key1  -H "Content-Type: application/json"`

`curl -X GET http://localhost:8001/get/key1  -H "Content-Type: application/json"`

**Тест кейс 4 (конкурентный update):**

`curl -X PATCH http://localhost:8000/update -H "Content-Type: application/json" -d '{"updates": {"key1": "value3"}}'`

`curl -X PATCH http://localhost:8001/update -H "Content-Type: application/json" -d '{"updates": {"key1": "value4"}}'`

`curl -X GET http://localhost:8000/get/key1  -H "Content-Type: application/json"`

`curl -X GET http://localhost:8001/get/key1  -H "Content-Type: application/json"`

**Тест кейс 5 (delete):**

`curl -X DELETE http://localhost:8000/delete/key2 -H "Content-Type: application/json"`

`curl -X GET http://localhost:8000/get/key2  -H "Content-Type: application/json"`

`curl -X GET http://localhost:8001/get/key2  -H "Content-Type: application/json"`

**Тест кейс 5 (конкурентный update и delete):**

`curl -X PATCH http://localhost:8000/update -H "Content-Type: application/json" -d '{"updates": {"key1": "value5"}}'`

`curl -X DELETE http://localhost:8000/delete/key1 -H "Content-Type: application/json"`

`curl -X GET http://localhost:8000/get/key1  -H "Content-Type: application/json"`

`curl -X GET http://localhost:8001/get/key1  -H "Content-Type: application/json"`

**Тест кейс 6 (каждая реплика исполняют свою операцию и синхронизирует их, проверяем, что все они в одном стейте - convergence):**

`python hw3/server.py    `

`PORT=8001 python hw3/server.py `

`PORT=8002 python hw3/server.py `

`curl -X POST http://localhost:8000/register -H "Content-Type: application/json" -d '{"url": "http://localhost:8001"}'`

`curl -X POST http://localhost:8000/register -H "Content-Type: application/json" -d '{"url": "http://localhost:8002"}'`

`curl -X POST http://localhost:8001/register -H "Content-Type: application/json" -d '{"url": "http://localhost:8000"}'`

`curl -X POST http://localhost:8001/register -H "Content-Type: application/json" -d '{"url": "http://localhost:8002"}'`

`curl -X POST http://localhost:8002/register -H "Content-Type: application/json" -d '{"url": "http://localhost:8000"}'`

`curl -X POST http://localhost:8002/register -H "Content-Type: application/json" -d '{"url": "http://localhost:8001"}'`

`curl -X PATCH http://localhost:8000/update -H "Content-Type: application/json" -d '{"updates": {"key1": "value1"}}'`

`curl -X PATCH http://localhost:8001/update -H "Content-Type: application/json" -d '{"updates": {"key1": "value2"}}`

`curl -X DELETE http://localhost:8002/delete/key1 -H "Content-Type: application/json"`

`curl -X GET http://localhost:8000/get/key1  -H "Content-Type: application/json"`

`curl -X GET http://localhost:8001/get/key1  -H "Content-Type: application/json"`

`curl -X GET http://localhost:8002/get/key1  -H "Content-Type: application/json"`

**Тест кейс 7 (отключить одну реплику, выполнить действие, восстановить и проверить, что все синхронизируется):**

Отключаем 8001

`curl -X PATCH http://localhost:8000/update -H "Content-Type: application/json" -d '{"updates": {"key1": "value1"}}'`

`curl -X GET http://localhost:8000/get/key1  -H "Content-Type: application/json"`

`curl -X POST http://localhost:8000/register -H "Content-Type: application/json" -d '{"url": "http://localhost:8001"}'`

`curl -X POST http://localhost:8001/register -H "Content-Type: application/json" -d '{"url": "http://localhost:8000"}'`

`curl -X GET http://localhost:8001/get/key1  -H "Content-Type: application/json"`
