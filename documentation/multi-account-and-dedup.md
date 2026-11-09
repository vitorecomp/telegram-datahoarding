# Múltiplas Contas e Deduplicação Cruzada

O `tghoard` foi projetado desde a base para permitir que **duas ou mais contas do Telegram** salvem seus dados no mesmo diretório `--data-dir` (ex.: `./data`) sem duplicar trabalho de rede nem espaço em disco.

## 1. Identidade Canônica de Pastas (`chat_key`)

| Tipo de Diálogo | Formato do `chat_key` | Comportamento entre Conta A (`111`) e Conta B (`222`) |
| :--- | :--- | :--- |
| **Canal (`Channel` broadcast)** | `channel_<channel_id>` | **Mesma pasta e mesma linha no `chats.md`**. `msg_id` é global no canal. |
| **Supergrupo / Fórum (`Megagroup`)** | `supergroup_<channel_id>` | **Mesma pasta e mesma linha no `chats.md`**. `msg_id` é global no supergrupo. |
| **Grupo Básico Legado (`Chat`)** | `group_<chat_id>` | **Mesma pasta e mesma linha no `chats.md`**. `msg_id` é por conta; deduplica por fingerprint determinístico. |
| **Chat Privado 1:1 (`User` / `Bot`)** | `dm_<min_id>_<max_id>` |Conversas com terceiros ficam isoladas (`dm_111_333` vs. `dm_222_333`), mas conversa direta entre Conta A e Conta B (`dm_111_222`) compartilha a mesma pasta sem duplicar. |
| **Saved Messages (`Self`)** | `saved_<self_id>` | Isolada por conta (`saved_111` e `saved_222`). |

## 2. Como Evitamos Re-Download de Mensagens

### Canais e Supergrupos (`chat_synced_ranges`)
No protocolo MTProto, canais e supergrupos possuem uma numeração sequencial única de mensagens (`1, 2, 3, ...`) compartilhada entre todos os participantes:
1. Quando a **Conta A** sincroniza o intervalo `[1, 5400]`, o `checkpoint.db` grava `(chat_key='supergroup_9876543210', start_id=1, end_id=5400)` em `chat_synced_ranges`.
2. Quando a **Conta B** passa pelo mesmo grupo em seguida, ela consulta o maior `msg_id` atual do servidor (ex.: `5405`) e calcula `missing_ranges([1, 5405], synced=[[1, 5400]]) -> [[5401, 5405]]`.
3. A Conta B baixa **apenas** as 5 mensagens novas (`5401..5405`) — ou eventuais faixas mais antigas caso a Conta B seja membro mais antigo e enxergue histórico anterior ao da Conta A.

### Grupos Básicos Legados e Conversas Diretas entre Contas Locais
Em grupos básicos legados (`PeerChat`) e DMs entre duas contas locais, cada conta tem seu próprio contador `msg_id` na caixa de entrada pessoal. Nesses casos:
- Cada mensagem recebe uma `canonical_key` baseada em fingerprint determinístico:
  `fp:<sha256(sender_id | timestamp_iso | media_global_key | text)>`
- Se a Conta B encontrar uma mensagem cujo `canonical_key` já existe em `message_locator`, ela apenas acrescenta seu ID em `"account_msg_ids"` e `"seen_by_accounts"` sem duplicar a mensagem nem re-baixar os anexos.

## 3. Como Evitamos Re-Download de Fotos, Vídeos, Áudios e Arquivos

No MTProto, os identificadores `photo.id` e `document.id` são **idênticos globalmente** entre diferentes contas do Telegram (apenas o `access_hash` muda por sessão):

1. **Mesmo chat**: antes de chamar `download_media`, o `media_manager` consulta `media_index` por `(tg_type, tg_id)`. Se o arquivo já está na pasta `photos/`, `videos/`, `audio/` ou `files/` daquele chat, retorna o caminho existente imediatamente (**0 bytes de rede**).
2. **Encaminhado entre chats diferentes**: se o `(tg_type, tg_id)` já foi baixado anteriormente na pasta de **outro** chat dentro de `data/`, o `media_manager` cria um **hardlink local (`os.link`)** apontando para o mesmo inode (com fallback para cópia local caso esteja em outro filesystem). Assim, cada pasta de chat continua completa e autônoma, sem gastar banda do Telegram nem duplicar espaço em disco.
