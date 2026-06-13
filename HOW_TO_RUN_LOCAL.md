# Rezervae Connect — Como Rodar Localmente

## Pré-requisitos

- **Node.js >= 20** (`node -v` para verificar)
- **pnpm** instalado globalmente (`npm install -g pnpm`)
- **Docker Desktop** rodando

## Passo a passo

### 1. Subir infraestrutura (PostgreSQL + Redis)

```bash
cd /c/xampp/htdocs/rezervae-connect
docker compose up -d
```

Aguardar containers ficarem healthy:
```bash
docker compose ps
```

Serviços criados:
| Container | Imagem | Porta |
|-----------|--------|-------|
| rezervae-postgres | postgres:16-alpine | 5432 |
| rezervae-redis | redis:7-alpine | 6379 |

### 2. Instalar dependências

```bash
pnpm install
```

### 3. Verificar .env

O arquivo `.env` na raiz já vem configurado para dev. Valores importantes:

```env
DATABASE_URL=postgresql://rezervae:rezervae@localhost:5432/rezervae_connect
REDIS_URL=redis://localhost:6379
INTERNAL_SECRET=dev-secret
CORE_API_URL=http://localhost:8080
API_PORT=3100
WS_PORT=3101
ORCHESTRATOR_PORT=3102
NODE_ENV=development
```

### 4. Aplicar schema e popular banco

```bash
pnpm db:push    # cria/atualiza tabelas
pnpm db:seed    # popula dados iniciais (tenant + instâncias)
```

> **Nota:** `db:push` é idempotente. Se o banco já tem o schema, não faz nada destrutivo.

### 5. Subir todos os serviços

```bash
pnpm dev
```

Isso usa Turborepo para iniciar 4 apps em paralelo (watch mode):

| App | Porta | Descrição |
|-----|-------|-----------|
| API | 3100 | REST endpoints (Fastify) |
| WebSocket | 3101 | Socket.IO para eventos real-time |
| Orchestrator | 3102 | Roteamento de eventos e mensagens |
| Workers | — | BullMQ workers (send, incoming, campaign, etc.) |

### 6. Verificar saúde

```bash
curl http://localhost:3100/v1/health
```

## Alinhamento com Laravel (agendei-backend)

O `.env` do Laravel deve ter estes valores batendo com o Connect:

```env
CONNECT_ENABLED=true
CONNECT_API_URL=http://host.docker.internal:3100   # Docker → host
CONNECT_INTERNAL_SECRET=dev-secret                  # = INTERNAL_SECRET do Connect
```

> **Se Laravel roda fora do Docker** (ex: `php artisan serve`), usar `http://localhost:3100` ao invés de `host.docker.internal`.

## Comandos úteis

```bash
pnpm db:studio      # Abre Drizzle Studio (GUI do banco)
pnpm build          # Build de produção (todos os packages)
pnpm clean          # Limpa dist/ de todos os packages
```

## Parar tudo

```bash
# Ctrl+C no terminal do pnpm dev
docker compose down          # para containers (mantém dados)
docker compose down -v       # para containers E apaga volumes (banco limpo)
```

## Avisos Importantes

- **NUNCA usar PM2 localmente** — causa processos zombie Chrome/Node (incidente real 2026-06-10)
- **Workers rodam no terminal** via `pnpm dev`, nunca em background
- **Secrets devem bater** entre Connect e Laravel, senão dá 401 silencioso
- Se mudar `.env`, reiniciar o `pnpm dev`
