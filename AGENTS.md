# AGENTS.md — Diretrizes do Projeto `telegram-datahoarding` (`tghoard`)

Este documento define a arquitetura, invariantes obrigatórias e fluxos de trabalho para agentes de IA que mantenham ou evoluam este repositório.

## 1. Visão Geral

`tghoard` é uma aplicação **100% controlada via CLI** (Python 3.13 + Telethon + Typer + Rich) para arquivamento pessoal de uma ou múltiplas contas do Telegram.

- **Sem Web UI**: toda interação ocorre pela CLI (`tghoard`) e pela edição do arquivo Markdown `<data_dir>/chats.md`.
- **Arquitetura em Duas Camadas**:
  1. **Acervo do Usuário em Plain Text (`<data_dir>/<chat_folder>/`)**: JSONs diários particionados, `links.md`, `metadata/*.md` e pastas de mídia (`photos/`, `videos/`, `audio/`, `files/`).
  2. **Estado Interno em SQLite WAL (`<data_dir>/.tghoard/checkpoint.db` + `sessions/*.session`)**: exclusivamente para checkpoints ACID de paginação, intervalos sincronizados entre contas, localização `O(log N)` de mensagens (`msg_id -> (day_str, part_num)`), fila `pending_media` e deduplicação global de mídias (`media_index`).

## 2. Invariantes Obrigatórias (Nunca Quebrar)

1. **Parâmetro de Saída (`data_dir`)**:
   - Todos os artefatos gerados (`chats.md`, `.tghoard/` e pastas de chats) devem ser salvos dentro de `data_dir` (configurado via `TGHOARD_DATA_DIR` no `.env` ou `--data-dir` / `-d` na CLI; valor default: `./data`).
2. **Fluxo `start` → `chats.md` → `sync`**:
   - `tghoard start` cria ou mescla de forma **não-destrutiva** `<data_dir>/chats.md`.
   - Cada chat deve ter os 5 checkboxes: `[x] baixar | [ ] photos | [ ] videos | [ ] audio | [ ] files` (`baixar` marcado por padrão; os outros 4 desmarcados por padrão).
   - Reexecutar `tghoard start` **jamais** pode sobrescrever marcações `[x]`/`[ ]` já editadas pelo usuário para chats existentes.
3. **Nomenclatura Uniforme `-ptNNN.json`**:
   - Todo arquivo JSON em `<chat_folder>/chats/` **deve** seguir o padrão `YYYY-MM-DD-ptNNN.json` (ex.: `2026-11-09-pt000.json`), **inclusive arquivos pequenos que não precisaram ser quebrados** (`-pt000.json`).
   - Quando o tamanho serializado do dia ultrapassar `max_json_kb` (default `1024 KB`), quebrar sempre entre mensagens (`-pt000.json`, `-pt001.json`, ...), mantendo cada parte como um documento JSON independente e válido.
4. **Estrutura Obrigatória de Cada Pasta de Chat**:
   - Dentro de `<data_dir>/<chat_folder>/`:
     - `links.md`
     - `chats/` (`YYYY-MM-DD-ptNNN.json`)
     - `photos/` (fotos de chat, imagens estáticas não-sticker, fotos de stories/perfil)
     - `videos/` (vídeos, GIFs MP4, *video notes*/"bolinhas", vídeos de stories/perfil)
     - `audio/` (*voice notes* e arquivos de áudio/música)
     - `files/` (qualquer anexo que não seja `photo`, `video` nem `audio`: PDFs, ZIPs, docs, stickers, código, etc.)
     - `metadata/` (`info.md`, `members.md`, `topics.md`, `stories.md`, `deleted_and_edits.md`, `profile_history.md`, `presence.md`, `events_and_admin_log.md`)
5. **Imutabilidade Histórica**:
   - Eventos remotos de exclusão (`UpdateDeleteMessages`) **nunca** apagam mensagens ou arquivos locais; apenas marcam `"is_deleted": true` no JSON da parte correspondente e registram em `metadata/deleted_and_edits.md`.
   - Edições (`UpdateEditMessage`) **nunca** descartam o texto anterior; empurram a versão prévia para `"revisions": [...]`.
6. **Deduplicação Multi-Conta**:
   - Duas contas no mesmo canal (`channel_<id>`), supergrupo (`supergroup_<id>`) ou grupo básico (`group_<id>`) compartilham a **mesma pasta** e a **mesma entrada** no `chats.md`.
   - Mensagens já cobertas em `chat_synced_ranges` e mídias já registradas em `media_index` (`(tg_type, tg_id)`) não devem ser baixadas novamente da rede (utilizar hardlink `os.link` com fallback para cópia local quando a mesma mídia aparecer em pastas diferentes).
7. **Escrita Atômica e Segurança**:
   - Gravar arquivos `.json` e `.md` sempre via arquivo temporário no mesmo diretório + `os.replace`.
   - Nunca versionar `.env`, `/data/`, `*.session*` ou `*.db*` no Git. Diretório `.tghoard/sessions/` deve ter permissão `0700` e arquivos `.session` permissão `0600`.

## 3. Mapa do Código

- `src/tghoard/config.py`: Carregamento de `.env` (`pydantic-settings`) e parâmetros globais.
- `src/tghoard/storage/`:
  - `atomic.py`: Escrita atômica plain-text e helpers de intervalos.
  - `paths.py`: Identidade canônica (`chat_key`), slugs de pastas e paths `-ptNNN.json`.
  - `chats_manifest.py`: Parser tolerante e gerador/merger de `<data_dir>/chats.md`.
  - `checkpoint_db.py`: SQLite WAL (`checkpoint.db`), cursores, ranges, `message_locator`, `pending_media` e `media_index`.
  - `chat_writer.py`: Leitura/escrita particionada de `chats/YYYY-MM-DD-ptNNN.json`, revisões e soft-delete.
  - `markdown_writer.py`: Geração de `links.md` e `metadata/*.md`.
  - `media_manager.py`: Classificador determinístico (`photos`/`videos`/`audio`/`files`), hardlinks e download.
- `src/tghoard/telegram/`:
  - `accounts.py`: Autenticação e gerenciamento multi-conta (`SQLiteSession`).
  - `takeout.py`: Sessão Takeout (`InitTakeoutSessionRequest`) com fallback automático.
  - `throttler.py`: Controle de concorrência, *jitter* e espera automática em `FloodWaitError`.
- `src/tghoard/sync/`:
  - `discovery.py`: Implementação do `tghoard start`.
  - `pts_diff.py`: Replay offline de `getDifference` / `getChannelDifference`.
  - `messages_sync.py`: Sync incremental por intervalos faltantes e drenagem de `pending_media`.
  - `metadata_sync.py`: Sync de perfis, membros, tópicos, stories, Admin Log e presença.
  - `watcher.py`: Escuta em tempo real opcional (`tghoard watch`).
  - `rebuilder.py`: Reconstrução de `checkpoint.db` e Markdown a partir do plain text.
  - `orchestrator.py`: Orquestração multi-conta do `tghoard sync`.
- `src/tghoard/cli.py`: Definição completa dos subcomandos Typer.

## 4. Documentação Detalhada

Consulte sempre e mantenha sincronizados os documentos em [`documentation/`](./documentation/):
- `documentation/architecture.md`
- `documentation/data-formats.md`
- `documentation/multi-account-and-dedup.md`
- `documentation/cli-reference.md`

## 5. Comandos de Desenvolvimento e Verificação

```bash
# Executar suíte completa de testes
.venv/bin/pytest -v

# Executar linter e checagem de formatação
.venv/bin/ruff check src tests
```
