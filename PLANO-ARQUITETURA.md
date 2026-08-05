# Plano de Arquitetura — Sistema de Notificações Assíncronas

## 1. Visão geral

Dois microsserviços desacoplados via Kafka:

```
                    ┌──────────────────────┐
   HTTP  ──POST──▶  │   notification-api   │
                    │  (REST + Producer)   │
                    │   MySQL: channels    │
                    └──────────┬───────────┘
                               │ publish (AlertMessage)
                               ▼
                    ┌──────────────────────┐
                    │  Kafka topic         │
                    │  notifications.alerts│
                    └──────────┬───────────┘
                               │ consume
                               ▼
                    ┌──────────────────────┐
   HTTP  ──GET───▶  │   alert-processor    │
   /status          │ (Consumer + REST)    │
                    │  MySQL: event_store  │
                    │  Strategies EMAIL/…  │
                    └──────────┬───────────┘
                               │ on failure (max retries)
                               ▼
                    ┌──────────────────────┐
                    │ notifications.alerts │
                    │        .DLT (DLQ)    │
                    └──────────────────────┘
```

**Cada serviço tem seu próprio banco MySQL** (database-per-service) — reforça o desacoplamento.

---

## 2. Estrutura do projeto (Maven multi-módulo)

```
pleno/
├── docker-compose.yml
├── README.md
├── pom.xml                       # parent (packaging pom)
├── contracts/                    # módulo compartilhado: contrato da mensagem Kafka
│   └── src/main/java/.../AlertMessage.java, ChannelType.java, Priority.java
├── notification-api/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/.../
│       ├── domain/               # entidades JPA
│       ├── repository/
│       ├── service/
│       ├── web/                  # controllers + DTOs + advice
│       ├── messaging/            # producer
│       └── config/
└── alert-processor/
    ├── Dockerfile
    ├── pom.xml
    └── src/main/java/.../
        ├── domain/               # NotificationEvent (event store)
        ├── repository/
        ├── service/
        ├── strategy/             # AlertStrategy + EMAIL/SMS/PUSH
        ├── messaging/            # consumer + error handler/DLQ
        ├── web/                  # status controller
        └── config/
```

**Trade-off do módulo `contracts`:** compartilhar o POJO da mensagem entre serviços reduz drift de contrato, mas cria acoplamento de build. Alternativa mais "purista" seria duplicar o contrato em cada serviço. Para o escopo do desafio, o módulo compartilhado é mais limpo e será documentado como decisão consciente no README.

---

## 3. Contrato da mensagem Kafka (`AlertMessage`)

Publicado por `notification-api`, consumido por `alert-processor`.

```json
{
  "correlationId": "uuid",
  "channelId": "uuid",
  "channelType": "EMAIL",
  "clientId": 12345,
  "message": "Olá João Silva, sua fatura de Novembro/2025 está disponível.",
  "config": { "maxRetries": 3, "priority": "HIGH" },
  "createdAt": "2025-11-10T14:30:00Z"
}
```

- **Key da mensagem = `correlationId`** → garante ordenação por notificação na mesma partição e ajuda na idempotência.
- Serialização: JSON (`JsonSerializer`/`JsonDeserializer` do Spring Kafka) ou String com Jackson.
- Header Kafka `correlationId` também setado para propagação de log (ver seção 9).

---

## 4. Tópicos Kafka

| Tópico | Uso | Partições |
|--------|-----|-----------|
| `notifications.alerts` | principal | 3 (permite paralelismo) |
| `notifications.alerts.DLT` | dead letter (falhas após retries) | 1 |

Retry: feito **em memória** pelo `DefaultErrorHandler` com backoff (não precisa de tópico de retry dedicado para o escopo). O `@RetryableTopic` com tópicos de retry separados é mencionado no README como alternativa mais avançada.

---

## 5. Banco de dados

### 5.1 `notification-api` — database `channels_db`

```sql
CREATE TABLE channels (
    id          CHAR(36)     NOT NULL PRIMARY KEY,
    name        VARCHAR(150) NOT NULL,
    type        VARCHAR(20)  NOT NULL,          -- EMAIL | SMS | PUSH
    template    TEXT         NOT NULL,
    config      JSON         NOT NULL,
    active      BOOLEAN      NOT NULL DEFAULT TRUE,
    deleted     BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMP    NOT NULL,
    updated_at  TIMESTAMP    NOT NULL
);
```

**Unicidade `name`+`type` com soft delete** — ponto de atenção clássico:
- MySQL não tem índice único parcial. Solução adotada: **validação na camada de serviço** (query `existsByNameAndTypeAndDeletedFalse`) + índice único auxiliar usando coluna gerada.
- Alternativa documentada como trade-off: coluna `deleted_at` nullable e único em `(name, type, deleted_at)` — mas com NULL o MySQL permite duplicatas, então a checagem em app continua sendo a fonte de verdade.

### 5.2 `alert-processor` — database `events_db` (Event Store append-only)

```sql
CREATE TABLE notification_events (
    id             CHAR(36)    NOT NULL PRIMARY KEY,
    correlation_id CHAR(36)    NOT NULL,
    channel_type   VARCHAR(20) NOT NULL,
    client_id      BIGINT      NOT NULL,
    status         VARCHAR(20) NOT NULL,   -- RECEBIDO|PROCESSANDO|PROCESSADO|FALHA
    detail         TEXT,
    occurred_at    TIMESTAMP(3) NOT NULL,
    created_at     TIMESTAMP(3) NOT NULL,
    UNIQUE KEY uk_correlation_status (correlation_id, status),  -- idempotência
    INDEX idx_correlation (correlation_id)
);
```

- **Append-only:** apenas INSERT, nunca UPDATE/DELETE.
- **Status atual** = evento com maior `occurred_at` (ou ordem de transição) para o `correlationId`.
- **`UNIQUE (correlation_id, status)`** torna cada transição idempotente: reprocessar a mesma mensagem tenta reinserir `RECEBIDO`/`PROCESSADO` → viola unique → tratado como no-op.

---

## 6. API — contratos

### notification-api

| Método | Rota | Descrição | Sucesso |
|--------|------|-----------|---------|
| POST | `/channels` | cria canal | 201 |
| GET | `/channels` | lista (não deletados) | 200 |
| GET | `/channels/{id}` | detalhe | 200 / 404 |
| PUT | `/channels/{id}` | atualiza | 200 / 404 |
| DELETE | `/channels/{id}` | exclusão **lógica** | 204 |
| POST | `/channels/{channelId}/alerts` | dispara alerta | 202 |

**POST /channels/{channelId}/alerts** — fluxo:
1. Busca canal; 404 se não existe/deletado.
2. 409/422 se `active=false`.
3. Resolve template com `params`; se faltar placeholder → 400 estruturado.
4. Gera `correlationId` (UUID).
5. Publica `AlertMessage` no Kafka.
6. Retorna `202 { "correlationId": "...", "status": "ACCEPTED" }`.

**Erro estruturado (padrão RFC-7807-like):**
```json
{
  "timestamp": "2025-11-10T14:30:00Z",
  "status": 400,
  "error": "VALIDATION_ERROR",
  "message": "Parâmetro obrigatório ausente: billingMonth",
  "path": "/channels/{id}/alerts",
  "correlationId": "..."
}
```

### alert-processor

| Método | Rota | Descrição |
|--------|------|-----------|
| GET | `/alerts/{correlationId}/status` | histórico + status atual |

Resposta conforme RF-5 (lista de eventos + `currentStatus` derivado do último).

---

## 7. Strategy Pattern (RF-3) — extensibilidade

```java
public interface AlertStrategy {
    ChannelType type();                 // EMAIL / SMS / PUSH
    void process(AlertMessage msg);     // simulado: log / arquivo
}
```

- Uma implementação por tipo: `EmailAlertStrategy`, `SmsAlertStrategy`, `PushAlertStrategy` (todas `@Component`).
- `AlertStrategyResolver` recebe `List<AlertStrategy>` por injeção e monta um `Map<ChannelType, AlertStrategy>`.
- **Novo tipo = nova classe `@Component`**, zero alteração no consumer. Este é o ponto explicitamente avaliado.

---

## 8. Consumo, resiliência e idempotência (RF-6)

**Fluxo do consumer:**
```
1. Recebe AlertMessage
2. Idempotência: se já existe evento terminal (PROCESSADO/FALHA) p/ correlationId → ignora (ack)
3. INSERT evento RECEBIDO   (unique protege duplicata)
4. INSERT evento PROCESSANDO
5. strategy.process(msg)
6. sucesso → INSERT PROCESSADO ; exceção → propaga p/ error handler
```

**Retry + DLQ:**
- `DefaultErrorHandler` com `ExponentialBackOff` (ex.: maxRetries do config, backoff 1s→...).
- `DeadLetterPublishingRecoverer` → publica em `notifications.alerts.DLT` ao esgotar tentativas.
- Ao esgotar: registra evento `FALHA` no event store antes/junto do envio à DLQ.
- Exceções de negócio não-recuperáveis (ex.: tipo desconhecido) → direto para DLQ (`addNotRetryableExceptions`).

**Idempotência garantida por 3 camadas:**
1. Checagem de evento terminal antes de processar.
2. `UNIQUE (correlation_id, status)` no banco.
3. `@Transactional` por mensagem — falha faz rollback consistente.

---

## 9. Logging estruturado (RNF)

- `logstash-logback-encoder` → logs em **JSON**.
- `correlationId` no **MDC**:
  - `notification-api`: gera e coloca no MDC ao disparar.
  - Propagação: header Kafka `correlationId`.
  - `alert-processor`: interceptor/`RecordInterceptor` lê o header e popula MDC antes de processar.
- Todos os logs de um fluxo carregam o mesmo `correlationId` ponta a ponta.

---

## 10. Configuração externalizada (RNF)

- `application.yml` (default) + `application-docker.yml` (profile `docker`).
- Tudo sensível/ambiental via **env var** com default: `${DB_URL:...}`, `${KAFKA_BOOTSTRAP:...}`, `${DB_PASSWORD}`.
- No compose, os serviços sobem com `SPRING_PROFILES_ACTIVE=docker`.

---

## 11. Docker Compose (RF-7)

Serviços:
- `mysql-channels` (ou um MySQL com 2 databases via init script)
- `kafka` (KRaft mode, sem Zookeeper — mais simples/moderno) + criação automática dos tópicos
- `notification-api` (depende de mysql + kafka healthy)
- `alert-processor` (depende de mysql + kafka healthy)

Recursos: `healthcheck` em MySQL e Kafka, `depends_on: condition: service_healthy`, build multi-stage nos Dockerfiles.

**Subida com um comando:** `docker compose up --build`.

---

## 12. Estratégia de testes

| Tipo | Alvo | Ferramenta |
|------|------|-----------|
| Unit | resolução de template, uniqueness, soft delete, services | JUnit 5 + Mockito |
| Unit | `AlertStrategyResolver`, cada strategy | Mockito |
| Unit | lógica de idempotência | Mockito |
| Slice | controllers | `@WebMvcTest` |
| Integração | fluxo Kafka + MySQL end-to-end | **Testcontainers** (Kafka + MySQL) |

Testcontainers é o diferencial que prova o fluxo assíncrono real.

---

## 13. Ordem de implementação sugerida

1. **Scaffold** — parent pom, módulos, `contracts` (AlertMessage + enums).
2. **Infra** — docker-compose (MySQL + Kafka) + Dockerfiles + profiles.
3. **notification-api** — CRUD de canais + testes.
4. **notification-api** — disparo + producer + resolução de template + testes.
5. **alert-processor** — consumer + event store + strategies + status endpoint.
6. **alert-processor** — retry + DLQ + idempotência + testes.
7. **Logging JSON + correlationId** ponta a ponta.
8. **Testcontainers** (integração) + polimento.
9. **README** — execução, endpoints, decisões e trade-offs.
10. **Smoke test** completo via docker-compose.

---

## 14. Decisões de design (resumo p/ README)

| Decisão | Motivo | Trade-off |
|---------|--------|-----------|
| Database-per-service | desacoplamento real | mais infra |
| Módulo `contracts` compartilhado | evita drift de contrato | acoplamento de build |
| `correlationId` como key Kafka | ordenação + idempotência | — |
| Idempotência via unique + check terminal | simples e robusto | unique por transição |
| Kafka KRaft (sem Zookeeper) | setup moderno/simples | — |
| Retry em memória + DLT | suficiente ao escopo | sem tópicos de retry escalonados |
| Soft delete + checagem em app | limitação de unique parcial no MySQL | validação fora do banco |
```

Plano salvo em [PLANO-ARQUITETURA.md](PLANO-ARQUITETURA.md). Aqui está o resumo do que ele define para você validar:

## Pontos-chave para você aprovar antes de codar

**1. Estrutura** — Maven multi-módulo com um módulo `contracts` compartilhado (o POJO da mensagem Kafka + enums), mais os dois serviços. Cada serviço com seu próprio banco MySQL (database-per-service).

**2. Contrato Kafka** — `AlertMessage` com `correlationId` como key da mensagem (garante ordenação + ajuda idempotência). Tópicos: `notifications.alerts` + `notifications.alerts.DLT`.

**3. Modelagem crítica:**
- **Event Store append-only**: `UNIQUE (correlation_id, status)` — isso resolve idempotência e append-only de uma vez só. Status atual = último evento.
- **Soft delete + unicidade name/type**: resolvido na camada de serviço (limitação do MySQL com índice único parcial — documentado como trade-off).

**4. Idempotência em 3 camadas** — check de evento terminal + unique constraint + `@Transactional`. Esse é um dos pontos que mais "pega" na avaliação.

**5. Strategy pattern** — `AlertStrategy` + resolver por `Map<ChannelType, Strategy>`. Novo tipo = nova `@Component`, zero mudança no consumer.

**6. Resiliência** — `DefaultErrorHandler` + `ExponentialBackOff` + `DeadLetterPublishingRecoverer`.

**7. Kafka em modo KRaft** (sem Zookeeper) — compose mais enxuto e moderno.

## Decisões que valem a pena você confirmar

Antes de eu começar, três escolhas onde há alternativa legítima:


<invoke name="AskUserQuestion">
<parameter name="questions">[{"question": "Módulo contracts compartilhado ou contrato duplicado em cada serviço?", "header": "Contrato Kafka", "multiSelect": false, "options": [{"label": "Módulo compartilhado (Recomendado)", "description": "Um módulo 'contracts' com o AlertMessage e enums, usado pelos dois serviços. Evita drift, código mais limpo. Trade-off: leve acoplamento de build entre serviços."}, {"label": "Duplicar contrato", "description": "Cada serviço tem sua própria cópia do POJO da mensagem. Desacoplamento 'purista' de microsserviços. Trade-off: risco de divergência do contrato."}]}, {"question": "Um MySQL com 2 databases ou dois containers MySQL separados?", "header": "MySQL", "multiSelect": false, "options": [{"label": "Um MySQL, 2 databases (Recomendado)", "description": "Um único container com databases channels_db e events_db via script de init. Mais leve para subir localmente, menos recursos. Mantém isolamento lógico."}, {"label": "Dois containers MySQL", "description": "Um container por serviço. Isolamento físico total, mais fiel a produção. Trade-off: mais memória/CPU na máquina de avaliação."}]}, {"question": "Incluir testes de integração com Testcontainers?", "header": "Testcontainers", "multiSelect": false, "options": [{"label": "Sim, incluir (Recomendado)", "description": "Testes de integração com Kafka + MySQL reais via Testcontainers, provando o fluxo assíncrono end-to-end. Grande diferencial na avaliação. Adiciona ~0,5 dia."}, {"label": "Só testes unitários", "description": "JUnit 5 + Mockito cobrindo services, strategies, idempotência e template. Cumpre o requisito mínimo. Mais rápido, mas sem prova do fluxo real integrado."}]}]