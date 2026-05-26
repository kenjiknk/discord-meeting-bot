# Discord Meeting Bot

Bot para gravar calls do Discord, transcrever em tempo real usando Whisper e gerar um `.jsonl` ao final para resumo automático via Claude Code.

## O que faz

- Entra no canal de voz e captura o áudio de cada participante separadamente
- Transcreve a cada 20 segundos e posta no canal de texto configurado
- Ao encerrar, gera um arquivo `.jsonl` pronto para o Claude Code resumir a reunião
- Para automaticamente quando o canal de voz esvazia

## Requisitos

- Python 3.11+
- [ffmpeg](https://ffmpeg.org/) instalado no sistema
- Token de bot Discord com as permissões abaixo
- ~4 GB de RAM livre para o modelo Whisper `large-v3`

## Instalação

```bash
git clone https://github.com/kenjiknk/discord-meeting-bot
cd discord-meeting-bot

python -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

## Configuração

### 1. Criar o bot no Discord

1. Acesse [discord.com/developers/applications](https://discord.com/developers/applications)
2. **New Application** → dê um nome
3. Vá em **Bot** → **Reset Token** → copie o token
4. Em **Bot**, ative:
   - `SERVER MEMBERS INTENT`
   - `MESSAGE CONTENT INTENT`
5. Em **OAuth2 → URL Generator**, selecione:
   - Scopes: `bot`, `applications.commands`
   - Bot Permissions: `Connect`, `Speak`, `Send Messages`, `Attach Files`, `Read Message History`
6. Use a URL gerada para adicionar o bot ao seu servidor

### 2. Configurar o `.env`

```bash
cp .env.example .env
```

Edite o `.env`:

```env
DISCORD_TOKEN=seu_token_aqui

# ID do canal de texto onde a transcrição e o .jsonl serão postados
# Botão direito no canal → Copiar ID (requer Developer Mode ativado em Configurações → Avançado)
SUMMARY_CHANNEL_ID=

# Para slash commands aparecerem instantaneamente durante desenvolvimento
# (sem isso, leva até 1h para propagar globalmente)
# Botão direito no servidor → Copiar ID
TEST_GUILD_ID=
```

### 3. Ativar Developer Mode no Discord

Configurações → Avançado → **Modo de Desenvolvedor** → ativar.

Isso permite copiar IDs de canais e servidores com botão direito.

## Uso

### Iniciar o bot

```bash
source .venv/bin/activate
python bot.py
```

### Comandos

| Comando | Descrição |
|---------|-----------|
| `/record` | Entra no seu canal de voz e inicia a gravação |
| `/stop` | Para a gravação e posta o `.jsonl` no canal |
| `/status` | Mostra duração e participantes da gravação atual |
| `/leave` | Para a gravação (se ativa) e sai do canal |

**Fluxo básico:**
1. Entre em um canal de voz
2. Digite `/record` em qualquer canal de texto
3. O bot entra e começa a transcrever
4. As transcrições aparecem a cada ~20s no canal configurado
5. Digite `/stop` ao terminar — o bot posta o `.jsonl` no canal

### Gerar resumo com Claude Code

Ao final da gravação, o bot posta o arquivo `.jsonl` no canal. Para gerar o resumo:

```bash
# Baixe o .jsonl postado pelo bot, depois execute:
claude -p "$(jq -r '.params.messages[0].content' meeting_YYYYMMDD_HHMMSS.jsonl)"
```

Ou use o [Anthropic Batch API](https://docs.anthropic.com/pt/docs/build-with-claude/message-batches):

```bash
claude api messages-batches create --file meeting_YYYYMMDD_HHMMSS.jsonl
```

## Variáveis de ambiente

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `DISCORD_TOKEN` | — | Token do bot (obrigatório) |
| `SUMMARY_CHANNEL_ID` | canal atual | ID do canal para transcrição e `.jsonl` |
| `TEST_GUILD_ID` | — | ID do servidor para sync rápido de slash commands |
| `WHISPER_MODEL` | `large-v3` | Modelo Whisper (`tiny`, `base`, `small`, `medium`, `large-v3`) |
| `WHISPER_LANGUAGE` | `pt` | Idioma para transcrição |
| `WHISPER_THREADS` | `8` | Threads de CPU para o Whisper |
| `WHISPER_BATCH_SIZE` | `8` | Batch size da inferência |
| `STREAM_CHUNK_SECS` | `20` | Intervalo em segundos entre transcrições |
| `SAVE_WAV` | `false` | Salva `.wav` por participante após a gravação |

## Estrutura das gravações

Cada sessão cria um diretório em `recordings/`:

```
recordings/
└── session_20260526_103000/
    ├── transcript.txt              # transcrição bruta por participante
    └── meeting_20260526_103000.jsonl  # input para o Claude Code
```
