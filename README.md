# telegram-datahoarding (`tghoard`)

Ferramenta **100% controlada via CLI** em Python 3.13 + Telethon para arquivar contas do Telegram em arquivos **plain text** organizados por chat (`chats/YYYY-MM-DD-pt000.json`, `links.md`, `metadata/*.md`), pastas dedicadas por categoria de anexo (`photos/`, `videos/`, `audio/`, `files/`), **checkpoints transacionais em SQLite** e **deduplicação automática entre múltiplas contas**.

## Destaques

- **Fluxo orientado por `chats.md`**: `tghoard start` descobre todos os seus chats/grupos/canais e gera `./data/chats.md` com 5 checkboxes por chat:
  `- [x] baixar | [ ] photos | [ ] videos | [ ] audio | [ ] files`
- **Formato Plain-Text Previsível**:
  - `chats/YYYY-MM-DD-pt000.json` (`-pt001.json`, ... quando ultrapassa o tamanho alvo, padrão `1 MB` / `1024 KB`). Até dias pequenos recebem `-pt000.json` para facilitar loops em scripts externos.
  - `links.md` com todos os links enviados no chat.
  - `metadata/*.md` com dados gerais, membros, tópicos, stories, mensagens apagadas/edições, histórico de perfil, presença e eventos/Admin Log.
- **Multi-Conta sem Re-Download**: Armazene múltiplas contas na mesma pasta `data/`. Se duas contas participarem do mesmo grupo ou canal, mensagens e mídias já capturadas pela primeira conta não são baixadas novamente.
- **Checkpoints ACID em SQLite**: Banco interno `data/.tghoard/checkpoint.db` (modo `WAL`) permite retomar do ponto exato após `Ctrl+C`, queda de rede ou `FloodWait`, além de localizar instantaneamente o arquivo `-ptNNN.json` de mensagens apagadas ou editadas offline.

## Quickstart

### 1. Instalar

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

### 2. Configurar `.env`

Obtenha seu `api_id` e `api_hash` em <https://my.telegram.org> e preencha no `.env`:

```dotenv
TG_API_ID=12345678
TG_API_HASH=seu_api_hash_aqui
TGHOARD_DATA_DIR=./data
```

### 3. Autenticar e gerar `data/chats.md`

```bash
.venv/bin/tghoard start
# Para adicionar uma segunda conta Telegram na mesma pasta ./data:
.venv/bin/tghoard start --add-account --alias conta2
```

### 4. Escolher o que baixar e sincronizar

Edite `./data/chats.md` marcando `[x]` nas opções desejadas (`baixar`, `photos`, `videos`, `audio`, `files`) e execute:

```bash
.venv/bin/tghoard sync
```

## Documentação Completa

Consulte a pasta [`documentation/`](./documentation/):

- [Arquitetura e Checkpoints SQLite](./documentation/architecture.md)
- [Formatos de Dados e Iteração sobre `*-ptNNN.json`](./documentation/data-formats.md)
- [Múltiplas Contas e Deduplicação](./documentation/multi-account-and-dedup.md)
- [Referência Completa da CLI e `.env`](./documentation/cli-reference.md)
