"""
Gera um arquivo .jsonl no formato do Anthropic Messages Batch API.

O usuário submete o arquivo depois via Claude Code:
  claude api messages-batches create --file resumo.jsonl

Ou processa direto:
  claude -p "$(jq -r '.params.messages[0].content' resumo.jsonl)"
"""
import json
from datetime import datetime

_SYSTEM_PROMPT = """\
Você é um assistente especializado em resumir reuniões do Discord.
Gere um resumo estruturado em Markdown, em português, seguindo exatamente este formato:

# Resumo da Reunião — {data}

## Participantes
- Lista de participantes

## Pontos Principais
- Bullet points objetivos dos tópicos discutidos

## Decisões Tomadas
- Decisões concretas

## Próximos Passos
- Ações e responsáveis (se mencionados)

## Transcrição Completa
(transcrição por participante)

Se não houver decisões ou próximos passos claros, escreva "Nenhum identificado."\
"""


def generate_jsonl(
    transcript_parts: list[tuple[str, str]],
    session_date: datetime,
    output_path: str,
) -> None:
    """Gera um .jsonl com uma request de resumo para o Anthropic Batch API."""

    data_fmt = session_date.strftime("%d/%m/%Y %H:%M")

    transcript_block = "\n\n".join(
        f"[{username}]\n{text}" for username, text in transcript_parts
    )

    user_message = (
        f"Data da reunião: {data_fmt}\n\n"
        f"Transcrição:\n\n{transcript_block}\n\n"
        f"Gere o resumo estruturado conforme as instruções."
    )

    record = {
        "custom_id": f"meeting-{session_date.strftime('%Y%m%d-%H%M%S')}",
        "params": {
            "model": "claude-sonnet-4-6",
            "max_tokens": 4096,
            "system": _SYSTEM_PROMPT,
            "messages": [
                {"role": "user", "content": user_message}
            ],
        },
    }

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
