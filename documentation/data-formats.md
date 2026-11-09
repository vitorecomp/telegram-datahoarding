# Formatos de Dados e Estrutura de Pastas

Todos os itens gerados pelo `tghoard` ficam dentro da pasta configurada por `TGHOARD_DATA_DIR` / `--data-dir` (padrão: `./data` na raiz do repositório).

## 1. Manifesto `<data_dir>/chats.md`

Gerado pelo comando `tghoard start`, lista todos os diálogos descobertos nas contas cadastradas:

```markdown
- [x] baixar | [ ] photos | [ ] videos | [ ] audio | [ ] files — **Dev Brasil** (`@devbrasil`) [`supergroup_9876543210`] *(contas: conta1, conta2)*
```

- O parser aceita tanto `[ ]` quanto `[]` sem espaço, além de `[x]` e `[X]`.
- O identificador entre crases e colchetes (ex.: ``[`supergroup_9876543210`]``) é a chave canônica imutável (`chat_key`).

## 2. Estrutura de Cada Pasta de Chat

Cada chat com `[x] baixar` possui uma pasta `<data_dir>/<chat_key>__<slug>/` com a estrutura:

```text
supergroup_9876543210__dev-brasil/
├── links.md
├── chats/
│   ├── 2026-11-08-pt000.json
│   ├── 2026-11-09-pt000.json
│   └── 2026-11-09-pt001.json
├── photos/
├── videos/
├── audio/
├── files/
└── metadata/
    ├── info.md
    ├── members.md
    ├── topics.md
    ├── stories.md
    ├── deleted_and_edits.md
    ├── profile_history.md
    ├── presence.md
    └── events_and_admin_log.md
```

## 3. Particionamento de Mensagens (`chats/YYYY-MM-DD-ptNNN.json`)

- **Nomenclatura Uniforme**: Todos os arquivos em `chats/` terminam com `-ptNNN.json` (`3+ dígitos` iniciando em `-pt000`), **inclusive dias pequenos que não precisaram ser quebrados**.
- **Limite de Tamanho por Parte**: Configurado por `TGHOARD_MAX_JSON_KB` / `--max-json-kb` (padrão: `1024 KB` / `1 MB`). A quebra sempre acontece entre mensagens completas, preservando cada arquivo como um JSON válido independente.

### Exemplo de Iteração em Python

Graças ao sufixo `-pt000` obrigatório em todos os arquivos, iterar sobre todo o histórico de um chat em ordem cronológica exige apenas `sorted`:

```python
import json
from pathlib import Path

chats_dir = Path("data/supergroup_9876543210__dev-brasil/chats")

for part_file in sorted(chats_dir.glob("*-pt*.json")):
    payload = json.loads(part_file.read_text(encoding="utf-8"))
    for msg in payload["messages"]:
        print(payload["date"], msg["id"], msg["sender"]["name"], msg["text"])
```

### Esquema do Documento `YYYY-MM-DD-ptNNN.json`

```json
{
  "chat_key": "supergroup_9876543210",
  "chat_title": "Dev Brasil",
  "date": "2026-11-09",
  "part": 0,
  "part_tag": "pt000",
  "message_count": 1,
  "first_msg_id": 48219,
  "last_msg_id": 48219,
  "updated_at": "2026-11-09T20:15:00+00:00",
  "messages": [
    {
      "canonical_key": "id:48219",
      "id": 48219,
      "account_msg_ids": {
        "111111111": 48219
      },
      "date": "2026-11-09T14:22:10+00:00",
      "topic_id": 12,
      "sender": {
        "id": 555666777,
        "name": "Ana Silva",
        "username": "anasilva"
      },
      "text": "Olhem esse benchmark: https://example.com/bench",
      "entities": [
        {
          "type": "MessageEntityUrl",
          "offset": 22,
          "length": 25,
          "url": "https://example.com/bench"
        }
      ],
      "links": ["https://example.com/bench"],
      "reply_to_msg_id": 48210,
      "forward_from": null,
      "media": {
        "category": "photos",
        "tg_type": "photo",
        "tg_id": 5918273918273,
        "file_name": null,
        "mime_type": "image/jpeg",
        "size_bytes": 184320,
        "downloaded": true,
        "path": "photos/2026-11-09_48219_photo_5918273918273.jpg",
        "sha256": "8f434346..."
      },
      "reactions": {
        "summary": [{ "emoji": "🔥", "count": 4 }],
        "users": []
      },
      "poll": null,
      "service_action": null,
      "edit_date": null,
      "revisions": [],
      "is_deleted": false,
      "deleted_detected_at": null,
      "delete_source": null,
      "seen_by_accounts": ["111111111"]
    }
  ]
}
```

## 4. Classificação das 4 Pastas de Anexo

| Categoria | Pasta | Regra de Classificação |
| :--- | :--- | :--- |
| **`photos`** | `photos/` | `MessageMediaPhoto` (fotos de mensagens, Stories e perfil) e documentos de imagem estática (`image/*`) sem atributo de sticker |
| **`videos`** | `videos/` | Documentos com `DocumentAttributeVideo` (vídeos, GIFs animados MP4, *video notes*/"bolinhas") ou `video/*` não-sticker, além de vídeos de Stories/perfil |
| **`audio`** | `audio/` | Documentos com `DocumentAttributeAudio` (*voice notes* e faixas de música/áudio) ou `audio/*` |
| **`files`** | `files/` | Qualquer outro arquivo que não se enquadre em `photos`, `videos` nem `audio` (PDFs, ZIPs, documentos Office, código, stickers `.webp`/`.tgs`/`.webm`, vCards, etc.) |

## 5. `links.md` e `metadata/*.md`

- `links.md`: Todos os links extraídos de texto bruto, hyperlinks formatados, botões inline e legendas de Stories, agrupados por data.
- `metadata/info.md`: Identificação do chat, tipo, descrição/bio, estatísticas e tabela das contas sincronizadas.
- `metadata/members.md`: Lista de membros e administradores visíveis com cargos e datas.
- `metadata/topics.md`: Tópicos de Fórum / Monofórum.
- `metadata/stories.md`: Catálogo de Stories capturados antes de expirarem.
- `metadata/deleted_and_edits.md`: Relatório cronológico legível de mensagens apagadas preservadas e histórico de edições.
- `metadata/profile_history.md`: Linha do tempo de mudanças em título/nome, `@username`, bio e fotos de perfil.
- `metadata/presence.md`: Snapshots de status `Online` / `was_online` e eventos de atividade/digitação.
- `metadata/events_and_admin_log.md`: Eventos de serviço (entradas/saídas, chamadas com duração, fixações) e eventos recuperados do Admin Log de 48h.
