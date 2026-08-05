# Sobre a solução — decisões e caminho

> Um relato do que construí, na ordem em que as decisões foram tomadas, e o porquê de cada uma.

## O ponto de partida

Quando li o desafio, a primeira coisa que me chamou atenção não foi a lista de requisitos — foi a frase "trate esta entrega como um componente que será implantado em produção". Isso mudou minha abordagem: em vez de correr para fazer os endpoints funcionarem, comecei desenhando a arquitetura no papel e me perguntando onde esse tipo de sistema costuma quebrar de verdade: mensagem duplicada, mensagem perdida, mensagem venenosa, concorrência. As decisões que descrevo abaixo são, em boa parte, respostas a essas quatro perguntas.

## A estrutura: três módulos, dois serviços

Optei por um projeto Maven multi-módulo com os dois serviços pedidos (`notification-api` e `alert-processor`) e um terceiro módulo, `contracts`, que guarda apenas o contrato da mensagem que trafega no Kafka.

O `contracts` foi uma escolha consciente com trade-off assumido: compartilhar o POJO da mensagem cria um leve acoplamento de build entre os serviços — o purismo de microsserviços mandaria duplicar o contrato em cada um. Preferi o módulo compartilhado porque o risco real aqui é outro: produtor e consumidor divergirem silenciosamente sobre o formato da mensagem. Num time grande, com repositórios separados, eu resolveria isso com schema registry ou contract tests; no escopo deste desafio, o módulo compartilhado entrega a mesma garantia com muito menos maquinário.

Cada serviço tem seu próprio database (`channels_db` e `events_db`). Nenhum serviço lê tabela do outro — a única ponte entre eles é o Kafka. Usei um único container MySQL com dois schemas para não pesar a máquina de quem for avaliar, mas o isolamento lógico está lá.

## O disparo: por que o 202 precisa ser honesto

O fluxo do disparo parece simples: validar o canal, resolver o template, publicar no Kafka, devolver `202` com o `correlationId`. O detalhe que me importava era o que acontece quando o Kafka não está bem.

Minha primeira versão publicava fire-and-forget: chamava `send()` e devolvia o 202. Ao revisar, percebi o problema — se o broker rejeitasse a mensagem, eu já tinha respondido "aceito" com um correlationId que não existia em lugar nenhum. Para um sistema financeiro, esse é o pior tipo de bug: silencioso e irrastreável. A versão final aguarda a confirmação do broker com timeout curto (5 segundos) antes de responder; se a publicação falha, devolvo `503` com uma mensagem clara — e o cliente pode reenviar com segurança, porque nenhum correlationId foi confirmado. Também configurei `acks=all`, producer idempotente e `max.block.ms` baixo, para que broker fora do ar signifique "erro rápido e explícito", não "thread HTTP presa por 60 segundos".

A key da mensagem no Kafka é o próprio `correlationId`. Essa escolha pequena carrega o design todo: mesma key → mesma partição → mesmo consumidor → reentregas da mesma notificação nunca são processadas em paralelo. A idempotência (falo dela adiante) fica muito mais simples por causa disso.

Outra decisão do produtor: o template é resolvido **antes** de publicar. O consumidor recebe a mensagem final, pronta. Isso mantém o processador simples e genérico — ele não precisa conhecer templates, e o conhecimento de "como montar uma notificação" fica no serviço que gerencia os canais.

## A unicidade de canais: onde a maioria confia demais na aplicação

O desafio pede que não existam dois canais com mesmo `name` e `type`, com exclusão lógica. A solução ingênua é checar com um `exists` antes de inserir — e foi o que fiz primeiro, sabendo que era insuficiente: duas requisições concorrentes passam ambas na checagem e ambas inserem. `@Transactional` não resolve isso; só constraint de banco resolve.

O problema é que o MySQL não tem índice único parcial ("unique só onde deleted = false"). A saída que encontrei foi uma coluna gerada: `active_scope = IF(deleted, id, 'ACTIVE')`. Canais vivos compartilham o valor `'ACTIVE'` e colidem entre si na unique `(name, type, active_scope)`; canais deletados usam o próprio id — nunca colidem, e liberam o par name+type para recriação. Mantive a pré-checagem na aplicação apenas para dar mensagem de erro amigável no caso comum; a fonte de verdade é a constraint, e a violação é traduzida para `409`.

## O processador: extensibilidade era o requisito de verdade

O enunciado é explícito: a implementação do envio pode ser simulada, o que importa é a estrutura que permite novos tipos. Usei Strategy: uma interface `AlertStrategy` com `type()` e `process()`, três implementações (`EMAIL`, `SMS`, `PUSH`) anotadas com `@Component`, e um resolver que recebe a lista injetada pelo Spring e indexa por tipo. Adicionar um canal de WhatsApp amanhã é criar uma classe nova — nem o consumidor, nem o resolver, nem qualquer configuração mudam. Se chegar uma mensagem de um tipo sem estratégia, tratei como falha permanente: não adianta tentar de novo, vai direto para a DLQ.

## O event store: append-only levado a sério

Cada mudança de status gera um INSERT imutável — não existe UPDATE em lugar nenhum do processador. O estado atual é derivado do último evento do `correlationId`. Aqui teve um refinamento que só apareceu quando revisei com calma: eu ordenava os eventos por `occurred_at`, mas RECEBIDO e PROCESSANDO são gravados com milissegundos de diferença — podem empatar no timestamp, e aí a ordem (e portanto o "estado atual") ficaria indefinida. Adicionei uma coluna `seq` com AUTO_INCREMENT como ordem total de gravação. Timestamp conta a história para humanos; `seq` garante a ordem para a máquina.

Outra sutileza: cada gravação de evento roda em transação própria (`REQUIRES_NEW`). Se o processamento falhar no meio, os eventos RECEBIDO e PROCESSANDO já estão commitados — a trilha de auditoria sobrevive à falha, que era exatamente o propósito dela.

## Idempotência: três camadas, nenhuma sozinha

Kafka entrega com semântica at-least-once — reentrega é fato da vida, não exceção. Minha defesa tem três camadas: primeiro, um curto-circuito — se já existe evento `PROCESSADO` para aquele correlationId, a reentrega é ignorada; segundo, uma constraint `UNIQUE(correlation_id, status)` — mesmo que a checagem passasse numa corrida, o banco não deixa duplicar transição; terceiro, o particionamento por key — reentregas da mesma notificação caem na mesma partição e nunca disputam entre si. O resultado prático: at-least-once do transporte + idempotência do consumidor = effectively-once do ponto de vista do negócio, sem pagar o custo de exactly-once transacional do Kafka, que seria complexidade demais para o ganho aqui.

Assumo o trade-off dessa escolha: como cada transição é única por alerta, retries não deixam trilha própria no histórico. Documentei isso como limitação consciente.

## Resiliência: retry, DLQ e a mensagem venenosa

O retry usa backoff exponencial (1s, 2s, 4s... teto de 10s). Um detalhe que fiz questão de implementar: o desafio define `maxRetries` na configuração do canal — e seria fácil deixar esse campo decorativo, com um retry global. Em vez disso, o valor viaja dentro da mensagem e o error handler o lê por mensagem (`setBackOffFunction`); canais sem configuração usam o default global externalizado.

Quando os retries esgotam, um recoverer registra o evento `FALHA` no event store e publica a mensagem na DLQ — o ciclo de vida fica completo mesmo no pior caso, e a mensagem fica disponível para replay.

O caso da poison pill (mensagem que nem desserializa) merece menção: sem tratamento, ela trava o consumidor num loop infinito. Usei o `ErrorHandlingDeserializer` do Spring Kafka para transformá-la em falha tratável, e — detalhe que descobri testando — configurei a DLQ com dois serializers: bytes crus para poison pills e JSON para mensagens válidas. Sem isso, o payload original chegava à DLQ corrompido (base64 dentro de JSON), inutilizando o diagnóstico. Com isso, a mensagem venenosa chega à DLQ byte a byte igual à original, e o consumidor segue vivo processando as próximas.

## Observabilidade: o correlationId como fio condutor

Os logs saem em JSON (via logstash-encoder) no ambiente docker e em formato legível no dev local. O `correlationId` entra no MDC na borda de cada serviço — filtro HTTP na API, início do listener no processador — e a partir daí todo log daquele fluxo o carrega automaticamente. Na prática: um `grep` pelo correlationId em qualquer um dos dois serviços conta a história completa da notificação. Ele também viaja como header Kafka, o que permite inspecionar mensagens na DLQ sem desserializar o corpo.

## Configuração e infraestrutura

Nenhum valor de ambiente está hardcoded: tudo segue o padrão `${ENV_VAR:default}`, com profile `docker` para o compose. Os Dockerfiles são multi-stage (Maven compila, JRE 21 roda) com os POMs copiados antes do código-fonte — mudança de código não invalida o cache de dependências. O compose sobe tudo com um comando: MySQL, Kafka em modo KRaft (sem Zookeeper — um container a menos e o modo atual do Kafka), e os dois serviços com healthcheck e `depends_on` condicionado à infra estar saudável. Adicionei também um profile opcional `tools` com Kafka UI e Adminer, porque inspecionar tópico e banco visualmente ajuda muito na demonstração — mas mantive fora do sobe padrão para não pesar.

## Testes: unitário para regra, integração para o mundo real

A pirâmide ficou assim: testes unitários (JUnit 5 + Mockito) para as regras — resolução de template, unicidade com corrida, ciclo de eventos, curto-circuito de idempotência, resolução de estratégia — e testes de integração com Testcontainers subindo MySQL e Kafka **reais** para provar o que mock não prova: a mensagem chega ao tópico com a key certa, o ciclo persiste no banco de verdade, a reentrega não duplica, a poison pill termina na DLQ.

Escolhi Testcontainers em vez de H2/EmbeddedKafka justamente porque as partes mais críticas do projeto — a constraint de unicidade, a coluna gerada, a serialização — dependem do comportamento real do MySQL e do Kafka. Teste que passa contra um banco que não é o de produção é meia verdade.

Aqui teve um imprevisto que virou aprendizado: os testes de integração falhavam com um erro confuso de "Docker environment not found", mesmo com o Docker rodando. Investigando com chamadas diretas à API do Docker, descobri que o Docker Engine 29 removeu o suporte a versões antigas da API HTTP — e o Testcontainers gerenciado pelo Spring Boot 3.4 (1.20.x) ainda pinava uma versão removida. A correção foi fixar `testcontainers.version=1.21.4` no pom pai, documentada no README para quem esbarrar no mesmo problema.

## Lombok: adotei, mas com critério

Usei Lombok para eliminar o boilerplate que não carrega decisão: `@Getter` e construtor protegido nas entidades, `@RequiredArgsConstructor` nos beans com injeção por construtor, `@Slf4j` nos loggers. E deliberadamente **não** usei o que costuma causar problema: `@Data`/`@ToString`/`@EqualsAndHashCode` em entidade JPA (toString pode disparar lazy-loading; equals/hashCode com ID de aplicação quebra contratos em coleções) e `@Setter` em entidades — a mutação passa por métodos com nome de negócio (`update`, `markDeleted`), que protegem as invariantes. Os DTOs nem precisaram de Lombok: records do Java 21 já resolvem melhor.

## O que eu faria na sequência

Se este componente fosse mesmo para produção, minha lista seguinte seria: **outbox pattern** no produtor (hoje, entre gravar e publicar não há atomicidade — o outbox fecha essa janela); métricas com Micrometer/Prometheus e tracing distribuído com OpenTelemetry; um consumidor da DLQ com política de replay; segurança (TLS/SASL no Kafka, autenticação na API, secrets em vault); e contract tests entre os serviços se os times se separassem.

## Resumo em uma frase

Construí o sistema assumindo que mensagens duplicam, brokers caem e payloads corrompem — e fiz cada uma dessas falhas ter um comportamento explícito, testado e rastreável, em vez de torcer para que não aconteçam.
