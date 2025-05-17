## Запуск
```bash
docker compose up -d && sleep 10 && go run producer/main.go && go run consumer/main.go
```