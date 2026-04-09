import asyncio

from src.memory_taxonomy import (
    classify_memory_candidates_semantic,
    classify_memory_semantic_fallback,
    classify_memory_text,
    normalize_domain,
    summarize_domain_distribution,
    summarize_label_distribution,
)


def test_normalize_domain_aliases():
    assert normalize_domain("career") == "work"
    assert normalize_domain("family") == "relationships"
    assert normalize_domain("unknown-x") == "general"


def test_classify_memory_text_health():
    result = classify_memory_text("Recently out of hospital with kidney stones.")
    assert result.domain == "health"
    assert result.score > 0


def test_summarize_domain_distribution():
    counts = summarize_domain_distribution(
        [{"domain": "work"}, {"domain": "career"}, {"domain": "health"}, {"domain": None}]
    )
    assert counts["work"] == 2
    assert counts["health"] == 1
    assert counts["general"] == 1


def test_semantic_fallback_multilabel_health_goal():
    result = classify_memory_semantic_fallback("I want to lose weight and start running every day.")
    assert result.domain in {"health", "wellness", "goals", "routines"}
    assert result.intent in {"set_goal", "make_commitment"}
    assert result.memory_type in {"goal", "habit"}
    assert result.classification_method == "fallback"
    assert "health" in result.domain_scores or "wellness" in result.domain_scores


def test_classify_memory_candidates_semantic_without_llm_uses_fallback():
    rows = [{"text": "I feel exhausted and anxious lately.", "source": "graphiti"}]
    out = asyncio.run(
        classify_memory_candidates_semantic(
            rows,
            llm_client=None,
            enable_embedding_fallback=False,
        )
    )
    assert len(out) == 1
    assert out[0].intent in {"express_emotion", "share_update"}
    assert out[0].classification_method == "fallback"


def test_summarize_label_distribution():
    counts = summarize_label_distribution(
        [{"intent": "set_goal"}, {"intent": "set_goal"}, {"intent": "reflect"}, {"intent": None}],
        key="intent",
    )
    assert counts["set_goal"] == 2
    assert counts["reflect"] == 1
    assert counts["unknown"] == 1


def test_semantic_fallback_paraphrase_heavy_goal():
    result = classify_memory_semantic_fallback(
        "I keep circling around this idea of becoming calmer and more intentional with my mornings."
    )
    assert result.intent in {"set_goal", "reflect", "make_commitment"}
    assert result.memory_type in {"goal", "habit", "fact"}
    assert result.domain in {"goals", "routines", "wellness", "general"}


def test_semantic_fallback_emotional_indirect():
    result = classify_memory_semantic_fallback(
        "Everything feels loud inside my head and I cannot settle."
    )
    assert result.intent in {"express_emotion", "share_update"}
    assert result.memory_type in {"state", "fact"}
    assert result.domain in {"wellness", "worries", "general"}


def test_semantic_fallback_multilingual_spanish():
    result = classify_memory_semantic_fallback(
        "Quiero mejorar mi rutina de sueno y bajar mi estres esta semana."
    )
    assert result.domain in {"wellness", "routines", "goals", "general"}
    assert result.intent in {"set_goal", "share_update", "make_commitment"}


def test_semantic_fallback_multilingual_german():
    result = classify_memory_semantic_fallback(
        "Ich mache mir Sorgen um Geld und die Miete diesen Monat."
    )
    assert result.domain in {"finance", "worries", "general"}
    assert result.intent in {"express_emotion", "share_update", "reflect"}


def test_semantic_fallback_mixed_domain_work_relationship():
    result = classify_memory_semantic_fallback(
        "My product deadline slipped and my partner is upset that I keep working late."
    )
    assert result.domain in {"work", "relationships", "worries", "general"}
    assert result.memory_type in {"event", "relationship", "fact", "state"}


def test_classify_memory_candidates_semantic_records_audit_stats_without_llm():
    rows = [
        {"text": "I want to ship this release this week.", "source": "graphiti"},
        {"text": "I'm exhausted and anxious.", "source": "graphiti"},
    ]
    audit = {}
    out = asyncio.run(
        classify_memory_candidates_semantic(
            rows,
            llm_client=None,
            enable_embedding_fallback=False,
            semantic_enabled=True,
            audit_stats=audit,
        )
    )
    assert len(out) == 2
    assert audit.get("semantic_enabled") is True
    assert audit.get("embedding_enabled") is False
    assert audit.get("embedding_attempted") is False
    assert audit.get("llm_attempted") is False
    methods = audit.get("classification_method_counts") or {}
    assert methods.get("fallback") == 2


def test_classify_memory_candidates_semantic_can_be_disabled():
    rows = [{"text": "I need help deciding this.", "source": "graphiti"}]
    audit = {}
    out = asyncio.run(
        classify_memory_candidates_semantic(
            rows,
            llm_client=None,
            semantic_enabled=False,
            enable_embedding_fallback=True,
            audit_stats=audit,
        )
    )
    assert len(out) == 1
    assert out[0].classification_method == "fallback"
    assert audit.get("semantic_enabled") is False
