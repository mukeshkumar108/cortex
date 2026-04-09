from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional
import asyncio
import json
import math
import re

try:
    import openai
except Exception:  # pragma: no cover - optional dependency fallback
    openai = None

from .config import get_settings


CANONICAL_DOMAINS: List[str] = [
    "health",
    "wellness",
    "work",
    "goals",
    "worries",
    "relationships",
    "routines",
    "finance",
    "learning",
    "spirituality",
    "home",
    "general",
]

DOMAIN_ALIASES: Dict[str, str] = {
    "career": "work",
    "job": "work",
    "family": "relationships",
    "habit": "routines",
    "habits": "routines",
    "money": "finance",
}

DOMAIN_KEYWORDS: Dict[str, List[str]] = {
    "health": [
        "hospital", "doctor", "kidney", "pain", "injury", "medication",
        "symptom", "sick", "ill", "health",
    ],
    "wellness": [
        "sleep", "stress", "anxious", "anxiety", "mood", "burnout", "energy",
        "walk", "exercise", "hydration", "wellbeing", "wellness",
    ],
    "work": [
        "work", "job", "client", "deadline", "project", "launch", "startup",
        "product", "team", "meeting", "roadmap", "release",
    ],
    "goals": [
        "goal", "target", "aim", "milestone", "plan", "priority", "vision",
        "north star", "objective",
    ],
    "worries": [
        "worried", "worry", "concern", "afraid", "scared", "risk", "uncertain",
        "overwhelmed", "stuck", "blocker",
    ],
    "relationships": [
        "partner", "girlfriend", "boyfriend", "wife", "husband", "friend",
        "family", "daughter", "son", "mom", "dad", "relationship",
    ],
    "routines": [
        "daily", "routine", "habit", "consistency", "every day", "streak",
        "morning", "evening",
    ],
    "finance": [
        "money", "budget", "rent", "salary", "income", "expense", "debt",
        "financial", "saving",
    ],
    "learning": [
        "learn", "study", "course", "practice", "reading", "skill", "training",
    ],
    "spirituality": [
        "spiritual", "meditate", "meditation", "faith", "prayer", "meaning",
    ],
    "home": [
        "home", "house", "room", "kitchen", "flat", "apartment", "chores",
    ],
}

CANONICAL_INTENTS: List[str] = [
    "share_update",
    "set_goal",
    "ask_help",
    "express_emotion",
    "make_commitment",
    "reflect",
]

CANONICAL_MEMORY_TYPES: List[str] = [
    "fact",
    "preference",
    "goal",
    "habit",
    "state",
    "relationship",
    "event",
]

_INTENT_KEYWORDS: Dict[str, List[str]] = {
    "ask_help": ["how", "help", "what should", "advice", "can i", "could i"],
    "set_goal": ["want to", "goal", "target", "trying to", "i plan"],
    "make_commitment": ["i will", "i'll", "going to", "commit", "start"],
    "express_emotion": ["i feel", "i'm tired", "i am tired", "anxious", "upset", "sad", "angry"],
    "reflect": ["realized", "noticed", "i think", "pattern", "keeps happening"],
}

_MEMORY_TYPE_KEYWORDS: Dict[str, List[str]] = {
    "habit": ["every day", "daily", "routine", "habit", "usually", "always"],
    "goal": ["goal", "want to", "target", "trying to"],
    "preference": ["prefer", "please", "like when", "don't", "do not"],
    "relationship": ["my wife", "my husband", "my partner", "my friend", "my mom", "my dad"],
    "state": ["i feel", "i'm", "i am", "tired", "anxious", "stressed"],
    "event": ["today", "yesterday", "this morning", "last night"],
}

_DOMAIN_PROTOTYPES: Dict[str, List[str]] = {
    "health": [
        "physical health symptoms treatment recovery body pain medical condition",
        "salud fisica sintomas tratamiento recuperacion dolor enfermedad",
        "koerperliche gesundheit symptome behandlung genesung schmerz krankheit",
    ],
    "wellness": [
        "sleep stress mood energy rest burnout mental wellbeing",
        "sueno estres estado de animo energia descanso bienestar mental",
        "schlaf stress stimmung energie erholung wohlbefinden mental",
    ],
    "work": [
        "job work project team deadline client product startup career",
        "trabajo proyecto equipo fecha limite cliente producto carrera",
        "arbeit projekt team frist kunde produkt karriere",
    ],
    "goals": [
        "goal target objective milestone long term direction outcome",
        "meta objetivo hito direccion largo plazo resultado",
        "ziel vorgabe meilenstein langfristige richtung ergebnis",
    ],
    "worries": [
        "worry fear concern uncertainty risk anxiety overwhelmed stuck",
        "preocupacion miedo incertidumbre riesgo ansiedad agobiado bloqueado",
        "sorge angst unsicherheit risiko angstgefuehl ueberfordert festgefahren",
    ],
    "relationships": [
        "partner wife husband friend family parent sibling relationship conflict support",
        "pareja esposa esposo amigo familia madre padre hermano relacion conflicto apoyo",
        "partner ehefrau ehemann freund familie mutter vater geschwister beziehung konflikt",
    ],
    "routines": [
        "daily routine habit consistency every day recurring behavior",
        "rutina diaria habito consistencia cada dia comportamiento recurrente",
        "taegliche routine gewohnheit bestaendigkeit jeden tag wiederkehrend",
    ],
    "finance": [
        "money budget savings debt rent salary income expenses financial pressure",
        "dinero presupuesto ahorros deuda alquiler salario ingresos gastos presion financiera",
        "geld budget ersparnisse schulden miete gehalt einkommen ausgaben finanzdruck",
    ],
    "learning": [
        "learn study practice course skill training education",
        "aprender estudiar practicar curso habilidad formacion educacion",
        "lernen studieren ueben kurs faehigkeit training bildung",
    ],
    "spirituality": [
        "faith prayer meditation meaning spiritual practice belief",
        "fe oracion meditacion significado practica espiritual creencia",
        "glaube gebet meditation sinn spirituelle praxis ueberzeugung",
    ],
    "home": [
        "home house apartment room chores cleaning kitchen",
        "casa hogar apartamento cuarto tareas limpieza cocina",
        "zuhause wohnung zimmer haushalt putzen kueche",
    ],
    "general": [
        "general personal update neutral context memory fact",
    ],
}

_INTENT_PROTOTYPES: Dict[str, List[str]] = {
    "share_update": [
        "sharing what happened describing an update",
        "compartiendo una actualizacion de lo que paso",
        "ein update beschreiben was passiert ist",
    ],
    "set_goal": [
        "setting a goal desired future outcome",
        "definir una meta resultado futuro deseado",
        "ein ziel setzen gewuenschtes zukuenftiges ergebnis",
    ],
    "ask_help": [
        "asking for help guidance or advice",
        "pedir ayuda orientacion o consejo",
        "um hilfe orientierung oder rat bitten",
    ],
    "express_emotion": [
        "expressing emotional state feeling overwhelmed anxious sad angry",
        "expresar estado emocional sentirse agobiado ansioso triste enojado",
        "emotion ausdruecken ueberfordert aengstlich traurig wuetend",
    ],
    "make_commitment": [
        "committing to an action plan or promise",
        "comprometerse con una accion plan o promesa",
        "sich zu einer handlung einem plan oder versprechen verpflichten",
    ],
    "reflect": [
        "reflecting on patterns and meaning from experience",
        "reflexionar sobre patrones y significado de la experiencia",
        "ueber muster und bedeutung der erfahrung reflektieren",
    ],
}

_MEMORY_TYPE_PROTOTYPES: Dict[str, List[str]] = {
    "fact": [
        "stable factual statement about the user",
        "hecho estable sobre la persona",
        "stabile tatsache ueber die person",
    ],
    "preference": [
        "user preference communication style likes dislikes",
        "preferencia del usuario estilo de comunicacion gustos",
        "praeferenz des nutzers kommunikationsstil vorlieben",
    ],
    "goal": [
        "desired target future objective intention",
        "objetivo futuro deseado intencion",
        "gewuenschtes zukunftsziel absicht",
    ],
    "habit": [
        "recurring routine repeated behavior over time",
        "rutina recurrente comportamiento repetido",
        "wiederkehrende routine wiederholtes verhalten",
    ],
    "state": [
        "current emotional or physical state",
        "estado emocional o fisico actual",
        "aktueller emotionaler oder koerperlicher zustand",
    ],
    "relationship": [
        "relationship detail about another person in the user's life",
        "detalle de relacion sobre otra persona en la vida del usuario",
        "beziehungsdetail zu einer anderen person im leben des nutzers",
    ],
    "event": [
        "time bound event that happened recently",
        "evento temporal que ocurrio recientemente",
        "zeitgebundenes ereignis das kuerzlich passiert ist",
    ],
}


@dataclass
class DomainClassification:
    domain: str
    score: float
    evidence_terms: List[str]


@dataclass
class MemorySemanticInterpretation:
    domain: str
    domain_scores: Dict[str, float]
    intent: str
    memory_type: str
    confidence: float
    classification_method: str
    evidence_terms: List[str]


_TOKEN_RE = re.compile(r"[A-Za-z0-9]+(?:['’][A-Za-z]+)?")


def normalize_domain(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return "general"
    mapped = DOMAIN_ALIASES.get(raw, raw)
    if mapped in CANONICAL_DOMAINS:
        return mapped
    return "general"


def _normalize_intent(value: Any) -> str:
    raw = str(value or "").strip().lower()
    return raw if raw in CANONICAL_INTENTS else "share_update"


def _normalize_memory_type(value: Any) -> str:
    raw = str(value or "").strip().lower()
    return raw if raw in CANONICAL_MEMORY_TYPES else "fact"


def _normalize_confidence(value: Any, default: float = 0.55) -> float:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return default
    if n > 1.0:
        n = n / 100.0
    return max(0.0, min(1.0, n))


def _tokens(text: str) -> List[str]:
    return [t.lower() for t in _TOKEN_RE.findall(text or "")]


def _safe_parse_json_object(raw: Any) -> Optional[Dict[str, Any]]:
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        return None
    text = raw.strip().replace("\r", "")
    if not text:
        return None
    if text.startswith("```"):
        text = text.strip("`").strip()
    if not text.startswith("{"):
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            text = text[start:end + 1]
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        repaired = re.sub(r",(\s*[}\]])", r"\1", text)
        try:
            parsed = json.loads(repaired)
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None


def _keyword_domain_scores(text: str) -> Dict[str, float]:
    raw = (text or "").strip().lower()
    if not raw:
        return {"general": 1.0}

    token_set = set(_tokens(raw))
    matches: Dict[str, List[str]] = {}
    for domain, keywords in DOMAIN_KEYWORDS.items():
        terms = [term for term in keywords if (term in raw or term in token_set)]
        if terms:
            matches[domain] = terms

    if not matches:
        return {"general": 1.0}

    weights = {domain: float(len(terms)) for domain, terms in matches.items()}
    total = sum(weights.values()) or 1.0
    scores = {domain: round(weight / total, 3) for domain, weight in weights.items()}
    residual = max(0.0, 1.0 - sum(scores.values()))
    if residual > 0:
        scores["general"] = round(residual, 3)
    return scores


def _fallback_intent(text: str) -> str:
    lower = (text or "").strip().lower()
    if not lower:
        return "share_update"
    for intent, needles in _INTENT_KEYWORDS.items():
        if any(needle in lower for needle in needles):
            return intent
    return "share_update"


def _fallback_memory_type(text: str) -> str:
    lower = (text or "").strip().lower()
    if not lower:
        return "fact"
    for memory_type, needles in _MEMORY_TYPE_KEYWORDS.items():
        if any(needle in lower for needle in needles):
            return memory_type
    return "fact"


def classify_memory_semantic_fallback(text: Optional[str], source_hint: Optional[str] = None) -> MemorySemanticInterpretation:
    raw = (text or "").strip()
    if not raw:
        return MemorySemanticInterpretation(
            domain="general",
            domain_scores={"general": 1.0},
            intent="share_update",
            memory_type="fact",
            confidence=0.0,
            classification_method="fallback",
            evidence_terms=[],
        )

    domain_scores = _keyword_domain_scores(raw)
    if source_hint and str(source_hint).lower() == "loops" and "general" in domain_scores and len(domain_scores) == 1:
        domain_scores = {"goals": 0.65, "general": 0.35}

    ranked = sorted(domain_scores.items(), key=lambda kv: kv[1], reverse=True)
    primary_domain = normalize_domain(ranked[0][0] if ranked else "general")
    evidence_terms = [
        term
        for term in DOMAIN_KEYWORDS.get(primary_domain, [])
        if term in raw.lower()
    ][:4]

    top_score = float(ranked[0][1] if ranked else 0.0)
    return MemorySemanticInterpretation(
        domain=primary_domain,
        domain_scores={normalize_domain(k): float(v) for k, v in domain_scores.items()},
        intent=_fallback_intent(raw),
        memory_type=_fallback_memory_type(raw),
        confidence=max(0.35, min(0.95, 0.45 + (top_score * 0.45))),
        classification_method="fallback",
        evidence_terms=evidence_terms,
    )


def _normalize_domain_scores(payload: Any) -> Dict[str, float]:
    if not isinstance(payload, dict):
        return {}
    scores: Dict[str, float] = {}
    for key, value in payload.items():
        domain = normalize_domain(key)
        try:
            v = float(value)
        except (TypeError, ValueError):
            continue
        if v <= 0:
            continue
        scores[domain] = max(scores.get(domain, 0.0), min(1.0, v))
    total = sum(scores.values())
    if total <= 0:
        return {}
    return {k: round(v / total, 3) for k, v in scores.items()}


def _l2_norm(values: List[float]) -> float:
    return math.sqrt(sum(v * v for v in values))


def _cosine_similarity(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    denom = _l2_norm(a) * _l2_norm(b)
    if denom <= 0:
        return 0.0
    return max(-1.0, min(1.0, sum(x * y for x, y in zip(a, b)) / denom))


def _mean_embedding(vectors: List[List[float]]) -> List[float]:
    if not vectors:
        return []
    length = len(vectors[0])
    acc = [0.0] * length
    count = 0
    for vec in vectors:
        if not vec or len(vec) != length:
            continue
        count += 1
        for idx, value in enumerate(vec):
            acc[idx] += float(value)
    if count <= 0:
        return []
    return [value / float(count) for value in acc]


def _top_score_and_margin(ranked: List[tuple[str, float]]) -> tuple[float, float]:
    if not ranked:
        return 0.0, 0.0
    top = max(0.0, float(ranked[0][1] or 0.0))
    second = max(0.0, float(ranked[1][1] or 0.0)) if len(ranked) > 1 else 0.0
    return top, max(0.0, top - second)


def _softmax(scores: Dict[str, float], temperature: float = 0.25) -> Dict[str, float]:
    if not scores:
        return {}
    temp = max(0.05, float(temperature))
    max_v = max(scores.values())
    exps: Dict[str, float] = {}
    for key, value in scores.items():
        exps[key] = math.exp((float(value) - max_v) / temp)
    total = sum(exps.values()) or 1.0
    return {key: round(val / total, 3) for key, val in exps.items()}


async def _embed_texts(texts: List[str], model: str) -> Optional[List[List[float]]]:
    if not texts:
        return []
    if openai is None:
        return None

    def _blocking() -> List[List[float]]:
        settings = get_settings()
        client = openai.OpenAI(api_key=settings.openai_api_key, timeout=8.0)
        response = client.embeddings.create(model=model, input=texts)
        return [list(row.embedding) for row in response.data]

    try:
        return await asyncio.to_thread(_blocking)
    except Exception:
        return None


async def _classify_with_embeddings(
    items: List[Dict[str, Any]],
    model: str = "text-embedding-3-small",
) -> Optional[List[MemorySemanticInterpretation]]:
    texts = [str((item or {}).get("text") or "").strip() for item in (items or [])]
    if not texts:
        return []

    domain_proto_texts = [text for rows in _DOMAIN_PROTOTYPES.values() for text in rows]
    intent_proto_texts = [text for rows in _INTENT_PROTOTYPES.values() for text in rows]
    memory_proto_texts = [text for rows in _MEMORY_TYPE_PROTOTYPES.values() for text in rows]
    all_texts = texts + domain_proto_texts + intent_proto_texts + memory_proto_texts
    vectors = await _embed_texts(all_texts, model=model)
    if not vectors or len(vectors) != len(all_texts):
        return None

    item_vectors = vectors[: len(texts)]
    offset = len(texts)

    def _consume_prototypes(prototype_map: Dict[str, List[str]]) -> Dict[str, List[float]]:
        nonlocal offset
        out: Dict[str, List[float]] = {}
        for label, rows in prototype_map.items():
            count = len(rows)
            block = vectors[offset: offset + count]
            offset += count
            out[label] = _mean_embedding(block)
        return out

    domain_protos = _consume_prototypes(_DOMAIN_PROTOTYPES)
    intent_protos = _consume_prototypes(_INTENT_PROTOTYPES)
    memory_protos = _consume_prototypes(_MEMORY_TYPE_PROTOTYPES)

    out: List[MemorySemanticInterpretation] = []
    for idx, text in enumerate(texts):
        vec = item_vectors[idx]
        domain_scores_raw = {
            label: _cosine_similarity(vec, proto_vec)
            for label, proto_vec in domain_protos.items()
            if proto_vec
        }
        domain_scores = _softmax(domain_scores_raw, temperature=0.22)
        ranked_domains = sorted(domain_scores.items(), key=lambda kv: kv[1], reverse=True)
        primary_domain = normalize_domain(ranked_domains[0][0] if ranked_domains else "general")

        intent_scores = {
            label: _cosine_similarity(vec, proto_vec)
            for label, proto_vec in intent_protos.items()
            if proto_vec
        }
        ranked_intents = sorted(intent_scores.items(), key=lambda kv: kv[1], reverse=True)
        intent = _normalize_intent(ranked_intents[0][0] if ranked_intents else "share_update")

        memory_scores = {
            label: _cosine_similarity(vec, proto_vec)
            for label, proto_vec in memory_protos.items()
            if proto_vec
        }
        ranked_memory = sorted(memory_scores.items(), key=lambda kv: kv[1], reverse=True)
        memory_type = _normalize_memory_type(ranked_memory[0][0] if ranked_memory else "fact")

        intent_top, intent_margin = _top_score_and_margin(ranked_intents)
        memory_top, memory_margin = _top_score_and_margin(ranked_memory)
        domain_certainty = max(0.0, float(ranked_domains[0][1] if ranked_domains else 0.0))
        domain_certainty_norm = min(1.0, domain_certainty / 0.35)
        intent_margin_norm = min(1.0, intent_margin / 0.08)
        memory_margin_norm = min(1.0, memory_margin / 0.08)
        confidence = (
            0.28
            + (0.33 * min(1.0, intent_top))
            + (0.22 * min(1.0, memory_top))
            + (0.17 * domain_certainty_norm)
            + (0.14 * intent_margin_norm)
            + (0.11 * memory_margin_norm)
        )
        confidence = max(0.20, min(0.98, confidence))

        out.append(
            MemorySemanticInterpretation(
                domain=primary_domain,
                domain_scores=domain_scores or {"general": 1.0},
                intent=intent,
                memory_type=memory_type,
                confidence=confidence,
                classification_method="embeddings",
                evidence_terms=[],
            )
        )
    return out


async def classify_memory_candidates_semantic(
    items: List[Dict[str, Any]],
    llm_client: Optional[Any] = None,
    enable_embedding_fallback: bool = True,
    embedding_model: str = "text-embedding-3-small",
    semantic_enabled: bool = True,
    audit_stats: Optional[Dict[str, Any]] = None,
) -> List[MemorySemanticInterpretation]:
    fallback_keyword = [
        classify_memory_semantic_fallback(
            text=str((row or {}).get("text") or ""),
            source_hint=str((row or {}).get("source") or ""),
        )
        for row in (items or [])
    ]
    if isinstance(audit_stats, dict):
        audit_stats.clear()
        audit_stats["item_count"] = len(fallback_keyword)
        audit_stats["semantic_enabled"] = bool(semantic_enabled)
        audit_stats["embedding_enabled"] = bool(enable_embedding_fallback)
        audit_stats["embedding_attempted"] = False
        audit_stats["embedding_success"] = False
        audit_stats["embedding_failed"] = False
        audit_stats["llm_attempted"] = False
        audit_stats["llm_success"] = False
        audit_stats["llm_failed"] = False
        audit_stats["embedding_model"] = str(embedding_model or "")
        audit_stats["classification_method_counts"] = {}

    if not semantic_enabled:
        if isinstance(audit_stats, dict):
            audit_stats["classification_method_counts"] = summarize_label_distribution(
                [{"classification_method": row.classification_method} for row in fallback_keyword],
                key="classification_method",
                default="fallback",
            )
        return fallback_keyword

    if not items:
        if isinstance(audit_stats, dict):
            audit_stats["classification_method_counts"] = summarize_label_distribution(
                [{"classification_method": row.classification_method} for row in fallback_keyword],
                key="classification_method",
                default="fallback",
            )
        return fallback_keyword

    base = fallback_keyword
    if enable_embedding_fallback:
        if isinstance(audit_stats, dict):
            audit_stats["embedding_attempted"] = True
        embedded = await _classify_with_embeddings(items, model=embedding_model)
        if embedded and len(embedded) == len(base):
            base = embedded
            if isinstance(audit_stats, dict):
                audit_stats["embedding_success"] = True
        elif isinstance(audit_stats, dict):
            audit_stats["embedding_failed"] = True

    if llm_client is None:
        if isinstance(audit_stats, dict):
            audit_stats["classification_method_counts"] = summarize_label_distribution(
                [{"classification_method": row.classification_method} for row in base],
                key="classification_method",
                default="fallback",
            )
        return base

    payload = [
        {
            "id": idx,
            "text": str((row or {}).get("text") or "")[:280],
            "source": str((row or {}).get("source") or ""),
        }
        for idx, row in enumerate(items)
        if str((row or {}).get("text") or "").strip()
    ]
    if not payload:
        return base

    prompt = (
        "Interpret each memory candidate semantically. Return STRICT JSON only.\\n"
        "Do not use lexical keyword matching heuristics in your reasoning; infer semantics from meaning.\\n"
        "Schema: {\"items\":[{\"id\":0,\"intent\":\"...\",\"memory_type\":\"...\",\"domain_scores\":{\"health\":0.6,\"goals\":0.4},\"confidence\":0.78}]}\\n"
        "intent must be one of: share_update,set_goal,ask_help,express_emotion,make_commitment,reflect.\\n"
        "memory_type must be one of: fact,preference,goal,habit,state,relationship,event.\\n"
        "domain_scores may include multiple domains and should approximately sum to 1.\\n"
        f"CANDIDATES_JSON: {json.dumps(payload, ensure_ascii=True)}"
    )

    try:
        if isinstance(audit_stats, dict):
            audit_stats["llm_attempted"] = True
        raw = await llm_client._call_llm(
            prompt=prompt,
            max_tokens=800,
            temperature=0.1,
            task="generic",
        )
    except Exception:
        if isinstance(audit_stats, dict):
            audit_stats["llm_failed"] = True
            audit_stats["classification_method_counts"] = summarize_label_distribution(
                [{"classification_method": row.classification_method} for row in base],
                key="classification_method",
                default="fallback",
            )
        return base

    parsed = _safe_parse_json_object(raw)
    rows = parsed.get("items") if isinstance(parsed, dict) else None
    if not isinstance(rows, list):
        if isinstance(audit_stats, dict):
            audit_stats["llm_failed"] = True
            audit_stats["classification_method_counts"] = summarize_label_distribution(
                [{"classification_method": row.classification_method} for row in base],
                key="classification_method",
                default="fallback",
            )
        return base

    out = list(base)
    llm_updates = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            idx = int(row.get("id"))
        except Exception:
            continue
        if idx < 0 or idx >= len(out):
            continue
        fallback = out[idx]
        llm_scores = _normalize_domain_scores(row.get("domain_scores"))
        if not llm_scores:
            llm_scores = fallback.domain_scores
        ranked = sorted(llm_scores.items(), key=lambda kv: kv[1], reverse=True)
        out[idx] = MemorySemanticInterpretation(
            domain=normalize_domain(ranked[0][0] if ranked else fallback.domain),
            domain_scores=llm_scores,
            intent=_normalize_intent(row.get("intent") or fallback.intent),
            memory_type=_normalize_memory_type(row.get("memory_type") or fallback.memory_type),
            confidence=_normalize_confidence(row.get("confidence"), default=fallback.confidence),
            classification_method="llm_refined",
            evidence_terms=fallback.evidence_terms,
        )
        llm_updates += 1
    if isinstance(audit_stats, dict):
        audit_stats["llm_success"] = llm_updates > 0
        if llm_updates <= 0:
            audit_stats["llm_failed"] = True
        audit_stats["classification_method_counts"] = summarize_label_distribution(
            [{"classification_method": row.classification_method} for row in out],
            key="classification_method",
            default="fallback",
        )
    return out


def classify_memory_text(text: Optional[str], source_hint: Optional[str] = None) -> DomainClassification:
    semantic = classify_memory_semantic_fallback(text, source_hint=source_hint)
    return DomainClassification(
        domain=semantic.domain,
        score=float(max(semantic.domain_scores.values()) if semantic.domain_scores else 0.0),
        evidence_terms=semantic.evidence_terms,
    )


def summarize_domain_distribution(items: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for item in items:
        domain = normalize_domain((item or {}).get("domain"))
        counts[domain] = int(counts.get(domain) or 0) + 1
    return counts


def summarize_label_distribution(items: Iterable[Dict[str, Any]], key: str, default: str = "unknown") -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for item in items:
        value = str((item or {}).get(key) or "").strip().lower() or default
        counts[value] = int(counts.get(value) or 0) + 1
    return counts
