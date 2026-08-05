# Guia de Testes Manuais

Roteiro para validar todo o sistema — **via tela** (Swagger, Kafka UI, Adminer) ou **via terminal** (curl / PowerShell).

## 1. Subir o ambiente

```bash
docker compose up -d --build
```

Com as ferramentas visuais de inspeção (recomendado para testes manuais):

```bash
docker compose --profile tools up -d --build
```

> Porta 3306 ocupada por um MySQL local? Prefixe com `MYSQL_PORT=3307` (PowerShell: `$env:MYSQL_PORT="3307"`).

Aguarde os healthchecks (~40s) e confira:

```bash
docker compose ps
```

## 2. Telas disponíveis

| Ferramenta | URL | O que testar por ela |
|---|---|---|
| **Swagger UI — notification-api** | http://localhost:8080/swagger-ui.html | CRUD de canais e disparo de alertas |
| **Swagger UI — alert-processor** | http://localhost:8081/swagger-ui.html | Consulta de status por correlationId |
| **Kafka UI** | http://localhost:8090 | Tópicos, mensagens, DLQ, consumer group |
| **Adminer** (MySQL) | http://localhost:8091 | Tabelas `channels` e `notification_events` |

**Login no Adminer:** servidor `mysql` · usuário `app` · senha `app` · base `channels_db` ou `events_db`.

---

## 3. Roteiro guiado — fluxo feliz (via tela)

### 3.1 Criar um canal
No Swagger da porta 8080 → `POST /channels` → *Try it out*:

```json
{
  "name": "Fatura Disponível",
  "type": "EMAIL",
  "template": "Olá {{clientName}}, sua fatura de {{billingMonth}} está disponível.",
  "config": { "maxRetries": 3, "priority": "HIGH" },
  "active": true
}
```

✔ Esperado: `201` com `id` — **copie o `id`**.

### 3.2 Disparar um alerta
`POST /channels/{channelId}/alerts` com o `id` copiado:

```json
{
  "clientId": 12345,
  "params": { "clientName": "João Silva", "billingMonth": "Novembro/2025" }
}
```

✔ Esperado: `202` com `correlationId` — **copie o `correlationId`**.

### 3.3 Ver a mensagem no Kafka
Kafka UI (8090) → *Topics* → `notifications.alerts` → *Messages*.
✔ Esperado: mensagem com **key = correlationId** e o template já resolvido no campo `message`.

### 3.4 Consultar o ciclo de vida
Swagger da porta 8081 → `GET /alerts/{correlationId}/status`:
✔ Esperado: `currentStatus: PROCESSADO` e 3 eventos na ordem `RECEBIDO → PROCESSANDO → PROCESSADO`.

### 3.5 Ver o event store (append-only)
Adminer (8091) → base `events_db` → tabela `notification_events`:
✔ Esperado: 3 linhas para o correlationId, com `seq` crescente. Repare que **não há UPDATE** — cada status é uma linha nova.

### 3.6 Ver o processamento simulado nos logs

```bash
docker logs alert-processor | grep "\[EMAIL\]"
```

✔ Esperado: `[EMAIL] Enviando e-mail para clientId=12345 conteudo="Olá João Silva..."` — em JSON, com o mesmo `correlationId` do disparo.

---

## 4. Cenários de erro (Swagger ou terminal)

| # | Cenário | Como reproduzir | Esperado |
|---|---------|-----------------|----------|
| 1 | Canal duplicado | `POST /channels` de novo com mesmo `name`+`type` | `409 DUPLICATE_CHANNEL` |
| 2 | Param faltando | Disparo com `params` sem `billingMonth` | `400 TEMPLATE_RESOLUTION_ERROR` + campo faltante em `violations` |
| 3 | Canal inexistente | Disparo com channelId aleatório | `404 CHANNEL_NOT_FOUND` |
| 4 | Canal inativo | Crie canal com `"active": false` e dispare | `422 CHANNEL_INACTIVE` |
| 5 | Tipo inválido | `POST /channels` com `"type": "FAX"` | `400` |
| 6 | clientId negativo | Disparo com `"clientId": -1` | `400 VALIDATION_ERROR` |
| 7 | UUID malformado | `GET /alerts/nao-e-uuid/status` (8081) | `400 INVALID_PARAMETER` |
| 8 | correlationId inexistente | `GET /alerts/00000000-0000-0000-0000-000000000000/status` | `404 ALERT_NOT_FOUND` |
| 9 | Exclusão lógica | `DELETE /channels/{id}` e depois `GET` do mesmo id | `204`, depois `404`; no Adminer a linha continua com `deleted=1` |
| 10 | Recriação pós-delete | Recrie canal com mesmo `name`+`type` do deletado | `201` (constraint libera o par) |

---

## 5. Cenários avançados

### 5.1 Idempotência (reentrega da mesma mensagem)

No Kafka UI → tópico `notifications.alerts` → copie o conteúdo de uma mensagem já processada → *Produce Message* com a **mesma key (correlationId) e o mesmo valor**.

Ou por terminal:

```bash
docker exec -i notifications-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic notifications.alerts --property parse.key=true --property key.separator=:
```

Cole `SEU-CORRELATION-ID:{json da mensagem}` e Ctrl+C.

✔ Esperado: `GET /alerts/{correlationId}/status` **continua com 3 eventos** — nada duplicado no banco.

### 5.2 Poison pill → DLQ

Produza no tópico `notifications.alerts` (Kafka UI → *Produce Message*, ou console producer acima) um valor que não é JSON válido, ex.: `{isso nao e json`.

✔ Esperado:
- Kafka UI → tópico `notifications.alerts.DLT` → a mensagem aparece **byte a byte igual** à enviada;
- O consumidor **continua vivo**: um novo disparo normal processa em seguida.

### 5.3 Retry por canal

Crie um canal com `"config": { "maxRetries": 5 }` e dispare. O valor viaja na mensagem (veja no Kafka UI o campo `config`) e é usado pela política de retry do consumidor (backoff exponencial; mensagens sem config usam o default `KAFKA_RETRY_MAX_ATTEMPTS=3`).

### 5.4 Rastreabilidade ponta a ponta

```bash
docker logs notification-api | grep SEU-CORRELATION-ID
docker logs alert-processor | grep SEU-CORRELATION-ID
```

✔ Esperado: logs JSON nos **dois** serviços carregando o mesmo `correlationId`.

### 5.5 Broker indisponível → 503

```bash
docker stop notifications-kafka
```

Dispare um alerta (Swagger 8080).
✔ Esperado: `503 BROKER_UNAVAILABLE` em ~5s (nenhum correlationId "fantasma" é confirmado).

```bash
docker start notifications-kafka
```

---

## 6. Equivalentes por terminal (sem tela)

<details>
<summary><b>curl (bash)</b></summary>

```bash
# criar canal
curl -s -X POST http://localhost:8080/channels -H "Content-Type: application/json" -d '{"name":"Fatura","type":"EMAIL","template":"Olá {{n}}","config":{"maxRetries":3},"active":true}'

# disparar (troque CHANNEL_ID)
curl -s -X POST http://localhost:8080/channels/CHANNEL_ID/alerts -H "Content-Type: application/json" -d '{"clientId":1,"params":{"n":"Ana"}}'

# status (troque CID)
curl -s http://localhost:8081/alerts/CID/status
```

</details>

<details>
<summary><b>PowerShell</b></summary>

```powershell
$ch = Invoke-RestMethod -Method Post -Uri http://localhost:8080/channels -ContentType "application/json" -Body '{"name":"Fatura","type":"EMAIL","template":"Olá {{n}}","config":{"maxRetries":3},"active":true}'
$al = Invoke-RestMethod -Method Post -Uri "http://localhost:8080/channels/$($ch.id)/alerts" -ContentType "application/json" -Body '{"clientId":1,"params":{"n":"Ana"}}'
Start-Sleep 5
Invoke-RestMethod -Uri "http://localhost:8081/alerts/$($al.correlationId)/status" | ConvertTo-Json -Depth 5
```

</details>

---

## 7. Encerrar

```bash
docker compose --profile tools down -v
```
