"""
services/gemini_api.py
======================
Wrapper da API Google Gemini para o Samba Export Control Desk.
Integrado com RAG (Retrieval-Augmented Generation) para inteligência local offline.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from google import genai
from google.genai import types
from dotenv import load_dotenv

# Importação do Buscador Semântico (RAG)
try:
    from services.rag_search import buscar_contexto_corporativo
except ImportError:
    def buscar_contexto_corporativo(q): return ""

load_dotenv()

logger = logging.getLogger(__name__)

# ── Modelos (em ordem de preferência/disponibilidade) ─────────────────────────
_MODEL_CANDIDATES = [
    "models/gemini-2.5-flash",          # Melhor custo-benefício
    "models/gemini-2.5-flash-lite",     # Fallback rápido
    "models/gemini-2.0-flash",          # Modelo estável
    "models/gemini-2.0-flash-lite",     # Fallback leve
]
MODEL_DEFAULT = _MODEL_CANDIDATES[0]
MODEL_FAST    = MODEL_DEFAULT
MODEL_DEEP    = MODEL_DEFAULT

# Rate limit plano gratuito: 15 req/min → 1 req/4s
# Reduzido: apenas quando necessário (controlado por _RATE_SLEEP_ENABLED)
_RATE_SLEEP = 2.0          # reduzido de 4.0 → 2.0 s
_API_TIMEOUT = 45          # timeout HTTP em segundos (era ∞ → causa travamentos de 50min)

# ── Client helper ──────────────────────────────────────────────────────────────

def _get_client() -> genai.Client:
    """
    Retorna cliente Gemini com HTTP timeout configurado.
    Sem timeout o processo pode travar indefinidamente em conexões penduradas.
    """
    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY não definida no .env")
    try:
        # google-genai >= 0.8 suporta http_options com timeout em segundos
        return genai.Client(
            api_key=api_key,
            http_options={"timeout": _API_TIMEOUT},
        )
    except TypeError:
        # fallback para versões antigas que não aceitam http_options
        logger.warning("genai.Client não aceita http_options — rodando SEM timeout HTTP.")
        return genai.Client(api_key=api_key)

# ── System prompts ─────────────────────────────────────────────────────────────

_SAMBA_SYSTEM = """Você é o Agente Diretor de Operações da Samba Export.
Sua missão é converter caos de mensagens em negócios estruturados seguindo rigorosamente 
os manuais de marca, apostilas de açúcar e commodities da empresa.

DIRETRIZES:
1. Use a taxonomia oficial: BID (Compra), ASK (Venda).
2. Se faltar documentos (LOI/ICPO/Incoterm), gere um alerta para a equipe humana.
3. Responda de forma objetiva, profissional e em português brasileiro."""

_DOCUMENTAL_SYSTEM = """Você é o especialista jurídico sênior da Samba Export.
Sua missão é redigir minutas contratuais (SCO, FCO, SPA) baseadas na Apostila de Commodities 
e no Catálogo Oficial, garantindo conformidade com padrões ICC e mensagens SWIFT."""

# ── Funções Auxiliares de Retry ────────────────────────────────────────────────

def _call_with_retry(fn_factory, retries: int = 4, base_sleep: float = 5.0):
    """
    Executa fn_factory(model) com retry e fallback automático de modelo.
    Trata 429 (quota), 503 (overload) e 404 (modelo indisponível) trocando para o próximo.

    Limites (revisados para evitar travamentos):
      - retries: 4 (era 6) — reduz sleep acumulado máximo
      - base_sleep: 5.0s (era 8.0s) — sleep por tentativa: 5, 10, 15, 20s = 50s total
      - Timeout HTTP: controlado pelo _get_client() (_API_TIMEOUT = 45s)
    """
    from google.genai.errors import ServerError, ClientError
    import socket
    model_idx = 0
    for attempt in range(retries):
        model = _MODEL_CANDIDATES[model_idx % len(_MODEL_CANDIDATES)]
        try:
            return fn_factory(model)
        except (ServerError, ClientError) as e:
            err_str = str(e)
            is_retryable = "429" in err_str or "503" in err_str or "404" in err_str
            if not is_retryable:
                raise
            model_idx += 1
            next_model = _MODEL_CANDIDATES[model_idx % len(_MODEL_CANDIDATES)]
            code = "429" if "429" in err_str else ("503" if "503" in err_str else "404")
            wait = base_sleep * (attempt + 1)
            logger.warning(
                "Gemini %s em %s. Trocando para %s. Aguardando %.0fs...",
                code, model, next_model, wait,
            )
            time.sleep(wait)
        except (TimeoutError, socket.timeout, OSError) as e:
            # Timeout HTTP explícito — não faz sentido esperar mais, troca de modelo
            model_idx += 1
            logger.warning(
                "Gemini timeout (%.0fs) em %s. Trocando para modelo %d.",
                _API_TIMEOUT, model, model_idx % len(_MODEL_CANDIDATES),
            )
            if attempt >= retries - 1:
                raise RuntimeError(f"Gemini timeout em todos os modelos após {retries} tentativas.") from e
    raise RuntimeError(f"Gemini falhou após {retries} tentativas em todos os modelos disponíveis.")

# ── Funções Públicas ───────────────────────────────────────────────────────────

def ask_gemini(
    prompt: str,
    system: str = _SAMBA_SYSTEM,
    model: str = None,
    max_tokens: int = 2048,
    *,
    use_rag: bool = True,
) -> str:
    """
    Chama o Gemini com prompt livre.

    use_rag=False desativa a busca RAG — usar para geração de texto criativo
    (follow-ups, rascunhos) onde o conhecimento corporativo não agrega valor
    e o overhead de embedding é desnecessário.
    """
    time.sleep(_RATE_SLEEP)
    client = _get_client()
    config = types.GenerateContentConfig(system_instruction=system, temperature=0.1)

    # Injeta Contexto Corporativo (RAG) apenas quando relevante
    if use_rag:
        contexto = buscar_contexto_corporativo(prompt)
        full_prompt = f"{contexto}\n\nPERGUNTA/MENSAGEM:\n{prompt}" if contexto else prompt
    else:
        full_prompt = prompt

    response = _call_with_retry(lambda m: client.models.generate_content(model=m, contents=full_prompt, config=config))
    return response.text or ""

def ask_gemini_json(
    prompt: str,
    system: str = _SAMBA_SYSTEM,
    model: str = None,
    max_tokens: int = 2048,
    *,
    max_retries: int = 6,
    base_sleep: float = 8.0,
) -> dict:
    """
    `max_retries` / `base_sleep` controlam o ciclo interno de fallback de modelos.
    Defaults preservados (6 tentativas, 8s base) para os call sites existentes.
    O orquestrador de planilha passa `max_retries=1, base_sleep=0.0` para
    falhar rápido — quem decide se há retry é o LLMGateway externo.
    """
    time.sleep(_RATE_SLEEP)
    client = _get_client()
    config = types.GenerateContentConfig(
        system_instruction=system,
        temperature=0.1,
        response_mime_type="application/json"
    )

    # Injeta Contexto Corporativo (RAG)
    contexto = buscar_contexto_corporativo(prompt)
    full_prompt = f"{contexto}\n\nEXTRAIA OS DADOS DA SEGUINTE MENSAGEM EM JSON:\n{prompt}" if contexto else prompt

    response = _call_with_retry(
        lambda m: client.models.generate_content(model=m, contents=full_prompt, config=config),
        retries=max_retries,
        base_sleep=base_sleep,
    )
    try:
        return json.loads(response.text)
    except Exception:
        return {"error": "JSON parse error", "raw": response.text}

def extract_quote_data(
    message_text: str,
    sender: str = "",
    group: str = "",
    *,
    max_retries: int = 6,
    base_sleep: float = 8.0,
) -> dict:
    """Extrai dados com consciência das especificações oficiais (ANEC, GAFTA, ICUMSA)."""
    prompt = f"Remetente: {sender} | Grupo: {group}\nConteúdo: {message_text}"

    # O prompt mestre de extração (Taxonomia Samba)
    system_instr = f"{_SAMBA_SYSTEM}\nRetorne JSON com: direcao (BID/ASK), commodity, price, currency, volume, incoterm, location, instrumentalizacao (LOI/ICPO/SPA), due_diligence (bool), risk_score (0-100), alerta_grupo_interno (string)."

    return ask_gemini_json(
        prompt,
        system=system_instr,
        max_retries=max_retries,
        base_sleep=base_sleep,
    )

def analyze_deal_risk(deal_data: dict) -> dict:
    """Analisa risco cruzando os dados do deal com as normas da Apostila."""
    prompt = f"Analise o risco deste deal frente às normas da Samba Export:\n{json.dumps(deal_data, indent=2)}"
    system_instr = f"{_SAMBA_SYSTEM}\nRetorne JSON com: score, level, factors (lista), recommendation."
    return ask_gemini_json(prompt, system=system_instr)

def generate_document_draft(deal_data: dict, doc_type: str = "SCO") -> str:
    """Gera minuta jurídica usando os modelos oficiais injetados via RAG."""
    prompt = f"Gere um rascunho de {doc_type} para:\n{json.dumps(deal_data, indent=2)}"
    return ask_gemini(prompt, system=_DOCUMENTAL_SYSTEM)

# ── Function Calling / Tool Use (Samba Assistant) ──────────────────────────────


def _to_gemini_tool(tool_declarations: list[dict[str, Any]]) -> "types.Tool":
    """Converte declaracoes JSON-Schema (do ToolRegistry) para types.Tool do Gemini."""
    return types.Tool(function_declarations=tool_declarations)


def _serialize_part(part: Any) -> dict[str, Any]:
    """Converte uma Part do Gemini (function_call | text) em dict serializavel."""
    fc = getattr(part, "function_call", None)
    if fc:
        return {
            "type": "function_call",
            "name": fc.name,
            "args": dict(fc.args) if fc.args else {},
        }
    text = getattr(part, "text", None)
    if text:
        return {"type": "text", "text": text}
    return {"type": "unknown"}


def chat_with_tools(
    user_message: str,
    history: list[dict[str, Any]] | None = None,
    tool_declarations: list[dict[str, Any]] | None = None,
    tool_executor: Any = None,
    system: str = _SAMBA_SYSTEM,
    max_tool_rounds: int = 4,
) -> dict[str, Any]:
    """
    Chat multi-turn com function calling. Orquestra o loop:
      user -> LLM -> (function_call)? -> tool_executor -> LLM -> ... -> texto final.

    Args:
        user_message:       texto do usuario neste turno.
        history:            lista [{"role": "user"|"model", "parts": [{"text"|"function_call"|"function_response"}]}]
                            no formato nativo do Gemini. Passe `[]` para iniciar sessao.
        tool_declarations:  saida de `ToolRegistry.to_gemini_declarations()`.
        tool_executor:      callable `(name: str, args: dict) -> dict` — tipicamente
                            `lambda n, a: registry.execute(n, **a)`. Obrigatorio se
                            `tool_declarations` for passado.
        system:             system prompt. Default = persona Samba.
        max_tool_rounds:    teto de iteracoes LLM->tool->LLM para evitar loops.

    Returns:
        {
          "text":          resposta final em linguagem natural,
          "tool_calls":    lista de {name, args, result} efetivamente executadas,
          "history":       novo historico Gemini com os turnos desta rodada apensados,
                           pronto pra ser passado na proxima chamada.
        }
    """
    if history is None:
        history = []
    if tool_declarations and tool_executor is None:
        raise ValueError("tool_executor obrigatorio quando tool_declarations e passado.")

    time.sleep(_RATE_SLEEP)
    client = _get_client()
    tools = [_to_gemini_tool(tool_declarations)] if tool_declarations else None
    config = types.GenerateContentConfig(
        system_instruction=system,
        temperature=0.2,
        tools=tools,
    )

    # Apensa o turno do usuario ao historico (formato Gemini).
    conversation = list(history) + [
        {"role": "user", "parts": [{"text": user_message}]}
    ]

    tool_trace: list[dict[str, Any]] = []
    final_text: str = ""

    for _round in range(max_tool_rounds):
        response = _call_with_retry(
            lambda m: client.models.generate_content(
                model=m, contents=conversation, config=config,
            )
        )
        candidate = response.candidates[0] if response.candidates else None
        if candidate is None or not candidate.content or not candidate.content.parts:
            final_text = response.text or ""
            break

        parts = candidate.content.parts
        function_calls = [p for p in parts if getattr(p, "function_call", None)]

        # Apensa a resposta do modelo no historico (tudo — texto E function_calls).
        conversation.append({
            "role": "model",
            "parts": [_serialize_part(p) for p in parts],
        })

        # Sem tool call -> resposta final.
        if not function_calls:
            final_text = response.text or ""
            break

        # Executa cada tool call e insere o resultado como "function_response".
        for fc_part in function_calls:
            fc = fc_part.function_call
            name = fc.name
            args = dict(fc.args) if fc.args else {}
            logger.info("chat_with_tools: invocando tool=%s args_keys=%s", name, list(args.keys()))
            try:
                result = tool_executor(name, args)
            except Exception as e:
                logger.exception("chat_with_tools: tool %s falhou", name)
                result = {"error": str(e)}

            tool_trace.append({"name": name, "args": args, "result": result})
            conversation.append({
                "role": "user",
                "parts": [{
                    "function_response": {"name": name, "response": result},
                }],
            })
        # Loop volta pro LLM com o function_response para gerar a resposta final.
    else:
        logger.warning("chat_with_tools: max_tool_rounds=%d atingido sem resposta final.", max_tool_rounds)
        final_text = "[Limite de rodadas de tool atingido sem resposta final.]"

    return {
        "text": final_text,
        "tool_calls": tool_trace,
        "history": conversation,
    }


# Aliases para manter compatibilidade com códigos antigos
ask_claude = ask_gemini
ask_claude_json = ask_gemini_json