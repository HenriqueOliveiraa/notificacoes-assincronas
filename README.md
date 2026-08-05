# Sistema de Notificações Assíncronas

Solução de notificações assíncronas composta por **dois microsserviços** que se comunicam via **Apache Kafka**, com rastreabilidade ponta a ponta, persistência orientada a eventos (event sourcing) e resiliência no consumo (retry + DLQ + idempotência).

```
┌──────────────────────┐   publica    ┌───────────────────────┐   consome   ┌──────────────────────┐
│   notification-api   │─────────────▶│  Kafka                │────────────▶│   alert-processor    │
│  REST + Producer     │              │  notifications.alerts │             │  Consumer + REST     │
│  MySQL: channels_db  │              └───────────┬───────────┘             │  MySQL: events_db    │
└──────────────────────┘                          │ falha após retries      └──────────────────────┘
                                                   ▼
                                       notifications.alerts.DLT (DLQ)
```

## Sumário

- [Stack](#stack)
- [Como executar](#como-executar)
- [Fluxo de ponta a ponta (demo)](#fluxo-de-ponta-a-ponta-demo)
- [Endpoints](#endpoints)
- [Arquitetura e requisitos atendidos](#arquitetura-e-requisitos-atendidos)
- [Testes](#testes)
- [Decisões de design e trade-offs](#decisões-de-design-e-trade-offs)
- [Configuração externalizada](#configuração-externalizada)

---

## Stack

| Camada | Tecnologia |
|--------|-----------|
| Linguagem | Java 21 |
| Framework | Spring Boot 3.4 |
| Mensageria | Apache Kafka (Spring Kafka, modo KRaft) |
| Persistência | MySQL 8.4 + Flyway |
| Testes | JUnit 5, Mockito, Testcontainers |
| Infra | Docker Compose |
| Logging | Logback + logstash-encoder (JSON) |

Projeto Maven **multi-módulo**:

```
pleno/
├── contracts/          # contrato compartilhado da mensagem Kafka (AlertMessage + enums)
├── notification-api/   # CRUD de canais + disparo (produtor Kafka)
├── alert-processor/    # consumidor, event store, strategies, DLQ, consulta de status
├── infra/mysql/        # script de init do MySQL (cria os 2 bancos)
└── docker-compose.yml
```

---

## Como executar

**Pré-requisito:** Docker + Docker Compose. Não é necessário ter Java ou Maven instalados — o build ocorre dentro de um container Maven.

Suba todo o ambiente (MySQL, Kafka e os dois serviços) com **um único comando**:

```bash
docker compose up --build
```

Serviços disponíveis após a subida:

| Serviço | URL |
|---------|-----|
| notification-api | http://localhost:8080 |
| alert-processor | http://localhost:8081 |
| Kafka (host) | localhost:29092 |
| MySQL (host) | localhost:3306 |

Health checks: `http://localhost:8080/actuator/health` e `http://localhost:8081/actuator/health`.

Para parar e limpar:

```bash
docker compose down -v
```

> **Porta 3306 ocupada?** Se já houver um MySQL local rodando no host, suba com outra porta externa:
> `MYSQL_PORT=3307 docker compose up --build` (no PowerShell: `$env:MYSQL_PORT="3307"; docker compose up --build`).
> Isso afeta apenas o acesso a partir do host; os serviços se comunicam pela rede interna do compose.

---

## Fluxo de ponta a ponta (demo)

### 1. Criar um canal

```bash
curl -X POST http://localhost:8080/channels \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Fatura Disponível",
    "type": "EMAIL",
    "template": "Olá {{clientName}}, sua fatura de {{billingMonth}} está disponível.",
    "config": { "maxRetries": 3, "priority": "HIGH" },
    "active": true
  }'
```

Resposta `201 Created` com o canal criado (guarde o `id`).

### 2. Disparar um alerta

```bash
curl -X POST http://localhost:8080/channels/{channelId}/alerts \
  -H "Content-Type: application/json" \
  -d '{
    "clientId": 12345,
    "params": { "clientName": "João Silva", "billingMonth": "Novembro/2025" }
  }'
```

Resposta `202 Accepted`:

```json
{ "correlationId": "0f9c...e21", "status": "ACCEPTED" }
```

### 3. Consultar o status do processamento

```bash
curl http://localhost:8081/alerts/{correlationId}/status
```

```json
{
  "correlationId": "0f9c...e21",
  "channelType": "EMAIL",
  "clientId": 12345,
  "currentStatus": "PROCESSADO",
  "events": [
    { "status": "RECEBIDO",    "detail": "Mensagem consumida do tópico",                 "occurredAt": "..." },
    { "status": "PROCESSANDO", "detail": "Iniciando processamento via estratégia EMAIL", "occurredAt": "..." },
    { "status": "PROCESSADO",  "detail": "Processamento concluído com sucesso",          "occurredAt": "..." }
  ]
}
```

Nos logs do `alert-processor` você verá a linha simulando o envio: `[EMAIL] Enviando e-mail para clientId=12345 ...`.

---

## Endpoints

### notification-api (porta 8080)

| Método | Rota | Descrição | Sucesso | Erros |
|--------|------|-----------|---------|-------|
| `POST` | `/channels` | Cria um canal | `201` | `400` validação, `409` duplicado |
| `GET` | `/channels` | Lista canais (não deletados) | `200` | — |
| `GET` | `/channels/{id}` | Detalha um canal | `200` | `404` |
| `PUT` | `/channels/{id}` | Atualiza um canal | `200` | `400`, `404`, `409` |
| `DELETE` | `/channels/{id}` | Exclusão **lógica** | `204` | `404` |
| `POST` | `/channels/{channelId}/alerts` | Dispara um alerta | `202` | `400` params, `404` inexistente, `422` inativo, `503` broker indisponível |

**Canal** (`POST`/`PUT /channels`):

```json
{
  "name": "Fatura Disponível",
  "type": "EMAIL",                       // EMAIL | SMS | PUSH
  "template": "Olá {{clientName}}...",   // placeholders no formato {{chave}}
  "config": { "maxRetries": 3, "priority": "HIGH" },
  "active": true
}
```

- Não é possível criar dois canais **ativos** com o mesmo `name` + `type` (`409`).
- Canais inativos não aceitam disparo (`422`).
- `config` aceita chaves adicionais específicas por tipo (preservadas como JSON).

**Disparo** (`POST /channels/{channelId}/alerts`):

```json
{ "clientId": 12345, "params": { "clientName": "João Silva", "billingMonth": "Novembro/2025" } }
```

**Resposta de erro estruturada** (padrão único da API):

```json
{
  "timestamp": "2025-11-10T14:30:00Z",
  "status": 400,
  "error": "TEMPLATE_RESOLUTION_ERROR",
  "message": "Parâmetros obrigatórios ausentes para resolver o template: [billingMonth]",
  "path": "/channels/{id}/alerts",
  "correlationId": "…",
  "violations": [ { "field": "params.billingMonth", "message": "parâmetro obrigatório ausente" } ]
}
```

### alert-processor (porta 8081)

| Método | Rota | Descrição | Sucesso | Erros |
|--------|------|-----------|---------|-------|
| `GET` | `/alerts/{correlationId}/status` | Histórico completo + status atual | `200` | `404` |

---

## Arquitetura e requisitos atendidos

| Requisito | Onde/Como |
|-----------|-----------|
| **RF-1** CRUD de canais | `ChannelController` + `ChannelService`; unicidade `name`+`type`; **soft delete** |
| **RF-2** Disparo de alertas | `AlertController` → `AlertDispatchService`: valida canal ativo, resolve template, gera `correlationId`, publica no Kafka, retorna `202` |
| **RF-3** Processamento assíncrono extensível | **Strategy pattern**: `AlertStrategy` + `AlertStrategyResolver`. Novo tipo = nova `@Component`, zero alteração no consumidor |
| **RF-4** Event Store append-only | `notification_events` só recebe INSERT; status atual = último evento por `correlationId` |
| **RF-5** Consulta de status | `StatusController` → `StatusQueryService` |
| **RF-6** Resiliência | Retry com backoff exponencial + **DLQ** (`DefaultErrorHandler` + `DeadLetterPublishingRecoverer`) + **idempotência** |
| **RF-7** Infra como código | `docker-compose.yml` sobe tudo com um comando |
| **RNF** Logging estruturado | Logs JSON com `correlationId` propagado via header Kafka + MDC |
| **RNF** Configuração externalizada | `application.yml` + profile `docker` + variáveis de ambiente |

### Ciclo de vida e Event Sourcing (RF-4)

```
RECEBIDO → PROCESSANDO → PROCESSADO | FALHA
```

Cada transição gera um **registro imutável** em `notification_events`. Não há `UPDATE`. O estado atual é derivado do evento mais recente para o `correlationId`.

### Idempotência (RF-6)

Garantida em **três camadas**:

1. **Curto-circuito**: se já existe evento `PROCESSADO` para o `correlationId`, a reentrega é ignorada.
2. **Constraint de banco**: `UNIQUE (correlation_id, status)` impede duplicar qualquer transição.
3. **Ordenação por chave**: a key Kafka é o `correlationId`, então a mesma notificação cai sempre na mesma partição, processada por uma única thread — elimina concorrência para um dado alerta.

### Retry + DLQ (RF-6)

- `DefaultErrorHandler` com **backoff exponencial** (configurável).
- Ao esgotar as tentativas: registra evento terminal `FALHA` e republica a mensagem na DLQ (`notifications.alerts.DLT`).
- Falhas **não recuperáveis** (ex.: tipo sem estratégia) vão direto para a DLQ, sem retry.
- **Poison pills** (mensagens não desserializáveis) são isoladas via `ErrorHandlingDeserializer` e encaminhadas à DLQ, sem travar o consumo.

---

## Testes

```bash
# Requer Java 21 + Maven instalados localmente
mvn test
```

- **Unitários (JUnit 5 + Mockito):** resolução de template, regras de canal (unicidade, soft delete), disparo, resolver de estratégias, idempotência do processamento e consulta de status.
- **Integração (Testcontainers — Kafka + MySQL reais):**
  - `notification-api`: cria canal → dispara → verifica publicação no tópico; disparo em canal inativo retorna `422`.
  - `alert-processor`: consome → persiste ciclo `RECEBIDO/PROCESSANDO/PROCESSADO` → valida endpoint de status; **idempotência** em reentrega; **DLQ** para mensagem malformada.

> Os testes de integração exigem um Docker Engine disponível (o Testcontainers sobe os containers automaticamente).
>
> **Nota de compatibilidade:** o projeto fixa `testcontainers.version=1.21.4` no pom pai — a versão gerenciada pelo Spring Boot 3.4 (1.20.x) é incompatível com o Docker Engine 29+, que removeu versões antigas da API (`/v1.32/*` → HTTP 400).

---

## Decisões de design e trade-offs

| Decisão | Motivo | Trade-off assumido |
|---------|--------|--------------------|
| **Database-per-service** (channels_db / events_db) | Desacoplamento real entre serviços | Mais um banco lógico para operar |
| Módulo **`contracts`** compartilhado | Evita divergência do contrato da mensagem | Leve acoplamento de build entre serviços |
| `correlationId` como **key** do Kafka | Ordenação por notificação + base da idempotência | — |
| **Event sourcing append-only** com coluna `seq` (AUTO_INCREMENT) | Rastreabilidade completa; `seq` dá ordem total determinística (timestamps podem colidir no mesmo ms) | Estado atual derivado (query ordenada) |
| Idempotência via **unique + curto-circuito** | Simples, robusto e à prova de reentrega | Recorrência do mesmo status não gera novo evento (retries não deixam trilha própria) |
| **Publicação síncrona com timeout curto** antes do `202` | Garante que o alerta aceito existe no broker; falha vira `503` reenviável | +alguns ms de latência por requisição (ack `all`) |
| **Retry por canal** (`config.maxRetries` da mensagem via `setBackOffFunction`) com default global | Honra a configuração do canal exigida no RF-1 | Semântica: `maxRetries` = tentativas *além* da entrega original |
| Kafka em **modo KRaft** (sem Zookeeper) | Setup mais moderno e enxuto | — |
| **Unicidade name+type no banco** via coluna gerada (`IF(deleted, id, 'ACTIVE')` + UNIQUE) | Constraint real contra corrida de requisições concorrentes; MySQL não tem índice único parcial | Pré-checagem na aplicação mantida para mensagens de erro amigáveis |
| **Soft delete** | Histórico preservado; par name+type liberado após exclusão | `DELETE` repetido → 404 (não idempotente por escolha) |
| **Flyway** como fonte única do schema (`ddl-auto: none`) | Schema versionado e determinístico | Hibernate não valida drift automaticamente |
| Entidades com **`Persistable`** (ID gerado na aplicação) | Evita SELECT extra (merge) a cada INSERT | Flag `isNew` transiente nas entidades |
| **DLQ com serializer por tipo** (`byte[]` vs JSON) | Poison pills chegam à DLQ byte a byte, permitindo replay/diagnóstico fiel | Dois templates de produtor no consumer |

### Uso de Lombok

Adoção **criteriosa**, não indiscriminada:

- ✅ `@Getter` + `@NoArgsConstructor(access = PROTECTED)` nas entidades JPA
- ✅ `@RequiredArgsConstructor` nos beans Spring com injeção por construtor
- ✅ `@Slf4j` no lugar de declarações manuais de logger
- ❌ `@Data`/`@ToString`/`@EqualsAndHashCode` em entidades JPA — anti-pattern (toString pode disparar lazy-loading; equals/hashCode com ID de aplicação quebra contratos em coleções)
- ❌ `@Setter` em entidades — mutação apenas via métodos de comportamento (`update`, `markDeleted`), preservando invariantes
- DTOs continuam como `record` (zero boilerplate sem Lombok)

---

## Configuração externalizada

Nenhum valor sensível ou específico de ambiente está hardcoded. Tudo é resolvido por **variáveis de ambiente** com defaults e **profiles do Spring** (`default` para execução local, `docker` para o compose).

Principais variáveis (ver `.env.example` e `docker-compose.yml`):

| Variável | Serviço | Default |
|----------|---------|---------|
| `DB_URL`, `DB_USERNAME`, `DB_PASSWORD` | ambos | mysql local |
| `KAFKA_BOOTSTRAP` | ambos | `localhost:9092` / `kafka:9092` |
| `KAFKA_TOPIC_ALERTS` / `KAFKA_TOPIC_DLT` | ambos | `notifications.alerts` / `.DLT` |
| `KAFKA_RETRY_MAX_ATTEMPTS` | alert-processor | `3` |
| `KAFKA_RETRY_INITIAL_MS` / `_MULTIPLIER` / `_MAX_MS` | alert-processor | `1000` / `2.0` / `10000` |
| `SERVER_PORT` | ambos | `8080` / `8081` |
