# Referência da CLI e Variáveis `.env`

## Variáveis de Ambiente (`.env`)

O `tghoard` carrega automaticamente o arquivo `.env` localizado no diretório de trabalho atual:

| Variável | Default | Descrição |
| :--- | :--- | :--- |
| `TG_API_ID` | *(obrigatório p/ rede)* | `api_id` numérico obtido em <https://my.telegram.org> |
| `TG_API_HASH` | *(obrigatório p/ rede)* | `api_hash` obtido em <https://my.telegram.org> |
| `TGHOARD_DATA_DIR` | `./data` | Pasta onde todos os itens (`chats.md`, `.tghoard/`, pastas de chats) são salvos |
| `TGHOARD_MAX_JSON_KB` | `1024` | Tamanho máximo alvo (em KB) por arquivo `chats/YYYY-MM-DD-ptNNN.json` |
| `TGHOARD_MAX_MEDIA_MB` | `50` | Limite de tamanho por anexo em MB (`0` = sem limite) |
| `TGHOARD_CONCURRENCY` | `3` | Limite de downloads/requisições concorrentes |
| `TGHOARD_SYNC_MODE` | `auto` | Modo de sessão: `auto`, `takeout` ou `standard` |
| `TGHOARD_AUDIT_WINDOW` | `200` | Janela de mensagens recentes auditadas caso o `PTS` expire (`DifferenceTooLong`) |

---

## Comandos Principais

Todos os comandos aceitam `--data-dir <caminho>` (ou `-d <caminho>`) para sobrescrever `TGHOARD_DATA_DIR`.

### 1. `tghoard start`
Autentica conta (caso ainda não haja conta salva ou se `--add-account` for informado), descobre todos os diálogos e cria/atualiza `<data_dir>/chats.md`:

```bash
# Primeira execução (faz login se necessário e gera ./data/chats.md)
tghoard start

# Adicionar uma segunda conta Telegram compartilhando a mesma ./data
tghoard start --add-account --alias trabalho

# Usar outro diretório de saída
tghoard start --data-dir /mnt/nas/telegram-backup
```

### 2. `tghoard sync`
Lê `<data_dir>/chats.md` e sincroniza todos os chats marcados com `[x] baixar`, baixando mídias conforme as marcações `[x] photos`, `[x] videos`, `[x] audio` e `[x] files`:

```bash
# Sincronizar todas as contas respeitando ./data/chats.md
tghoard sync

# Sincronizar apenas uma conta específica ou filtrar um chat específico
tghoard sync --account trabalho
tghoard sync --chat supergroup_9876543210

# Ajustar tamanho de quebra dos JSONs ou limite de mídia na execução
tghoard sync --max-json-kb 512 --max-media-mb 100
```

### 3. `tghoard account`
Gerenciamento direto das contas autenticadas em `<data_dir>/.tghoard/`:

```bash
tghoard account list
tghoard account add --alias pessoal
tghoard account remove trabalho
```

### 4. `tghoard watch`
Modo opcional em tempo real (executa até `Ctrl+C`) para capturar eventos voláteis que o servidor do Telegram não retém offline (ex.: eventos de digitação `typing`, transições contínuas `Online/Offline` e mensagens apagadas segundos após o envio):

```bash
tghoard watch
```

### 5. `tghoard search` e `tghoard stats`
Inspeção rápida do acervo plain-text diretamente pelo terminal:

```bash
tghoard stats
tghoard search "benchmark"
tghoard search "confidencial" --deleted
tghoard search "https://" --links
```

### 6. `tghoard rebuild-checkpoint` e `tghoard rebuild-md`
Ferramentas de manutenção e portabilidade do acervo:

```bash
# Reconstrói data/.tghoard/checkpoint.db varrendo apenas os arquivos plain-text em data/
tghoard rebuild-checkpoint

# Regenera links.md e metadata/*.md a partir dos JSONs salvos
tghoard rebuild-md
```

---

## Boas Práticas contra `FloodWait` e Bloqueios

- Deixe `TGHOARD_SYNC_MODE=auto` (padrão): o `tghoard` tenta iniciar uma sessão oficial **Telegram Takeout** (`account.initTakeoutSession`), criada especificamente pelo Telegram para exportação de dados pessoais com limites de taxa relaxados.
- Em sessões recém-autenticadas, o Telegram pode exigir confirmação no aplicativo móvel ou aplicar espera de segurança de 24h para Takeout (`TakeoutInitDelayError`). Nesse caso, o modo `auto` prossegue automaticamente em modo padrão cadenciado, respeitando `FloodWaitError` com contagem regressiva e *jitter*.
- Evite valores altos em `TGHOARD_CONCURRENCY` (mantenha entre `2` e `4`).
