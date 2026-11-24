from __future__ import annotations

import json
import os
import re
import sys
import urllib.parse
from functools import lru_cache
from pathlib import Path
from typing import Any

import requests
from neo4j import GraphDatabase

try:
    from validacion import (
        IGNORED_ENTITY_TYPES,
        detect_preferred_relations,
        extract_alias_entities,
        fallback_domain_check,
        filter_llm_entities,
        select_query_terms,
        split_priority_relations,
        strip_json_fences,
    )
except ImportError:
    try:
        from app.validacion import (
            IGNORED_ENTITY_TYPES,
            detect_preferred_relations,
            extract_alias_entities,
            fallback_domain_check,
            filter_llm_entities,
            select_query_terms,
            split_priority_relations,
            strip_json_fences,
        )
    except ImportError:
        IGNORED_ENTITY_TYPES: set[str] = set()

        def strip_json_fences(raw_text: str) -> str:
            cleaned = raw_text.strip()
            if cleaned.startswith("```"):
                cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
                cleaned = re.sub(r"\s*```$", "", cleaned)
            return cleaned.strip()

        def extract_alias_entities(question: str) -> list[str]:
            return []

        def filter_llm_entities(llm_entities: list[str], alias_entities: list[str]) -> list[str]:
            seen: set[str] = set()
            merged: list[str] = []
            for ent in alias_entities + llm_entities:
                key = ent.lower()
                if key not in seen:
                    seen.add(key)
                    merged.append(ent)
            return sorted(merged, key=lambda s: s.lower())

        def select_query_terms(entities: list[str]) -> list[str]:
            return list(entities)

        def detect_preferred_relations(question: str) -> list[str]:
            return []

        def split_priority_relations(preferred_rels: list[str] | None) -> tuple[list[str], list[str]]:
            return [], list(preferred_rels or [])

        def fallback_domain_check(question: str) -> dict[str, Any]:
            return {
                "in_domain": False,
                "reason": "No se pudo validar el dominio de la consulta.",
            }

# ==========================================
# 1. CONFIGURACIÓN DE NEO4J, OLLAMA Y RUTAS
# ==========================================
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4j1234")
NEO4J_DB = os.getenv("NEO4J_DB", "oncopancreaticcdss")

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")

# Parámetros estrictos de determinismo para Ollama
OLLAMA_OPTIONS: dict[str, Any] = {
    "temperature": 0.0,
    "top_p": 0.1,
    "top_k": 1,
    "mirostat": 0,
    "repeat_penalty": 1.1,
    "seed": 42,
    "num_ctx": 4096,
}

BASE_DIR = Path(__file__).resolve().parent.parent
INTERIM_DATA_DIR = BASE_DIR / "data" / "02_interim"

NO_EVIDENCE_MSG = (
    "No dispongo de evidencia suficiente en el grafo de conocimiento actual "
    "para responder a esta consulta clínica."
)
OUT_OF_DOMAIN_MSG = (
    "Consulta fuera de dominio: OncoPancreaticCDSS solo responde preguntas "
    "relacionadas con oncología pancreática, fármacos, biomarcadores, "
    "mutaciones, toxicidades, ensayos clínicos o el propio sistema."
)

CATEGORY_LABELS: dict[str, str] = {
    "01_guias": "Guía Clínica",
    "02_papers": "Artículo Científico",
    "03_ensayos": "Ensayo Clínico",
    "04_farmacos": "Ficha Técnica de Fármaco",
}

# Las 5 preguntas clínicas estrella basadas en los datos verificados de Neo4j
STAR_CLINICAL_QUESTIONS: list[dict[str, str]] = [
    {
        "id": "q1_combinaciones_gemcitabina",
        "titulo": "Combinaciones con Gemcitabina y Nab-Paclitaxel",
        "categoria": "Combinación de fármacos",
        "pregunta": (
            "¿Con qué fármacos se combina la Gemcitabina (incluido Nab-Paclitaxel) "
            "para tratar el adenocarcinoma ductal pancreático?"
        ),
    },
    {
        "id": "q2_regimenes_folfirinox_nalirifox",
        "titulo": "Regímenes FOLFIRINOX, mFOLFIRINOX y NALIRIFOX",
        "categoria": "Selección de tratamiento y toxicidad",
        "pregunta": (
            "¿Qué indicaciones, ensayos clínicos y toxicidades presentan los regímenes "
            "FOLFIRINOX, mFOLFIRINOX y NALIRIFOX en el cáncer de páncreas?"
        ),
    },
    {
        "id": "q3_kras_terapias_dirigidas",
        "titulo": "Terapias dirigidas en mutaciones KRAS G12D y KRAS G12C",
        "categoria": "Mutaciones y terapias dirigidas",
        "pregunta": (
            "¿Qué terapias dirigidas y ensayos clínicos existen para las mutaciones "
            "KRAS G12D y KRAS G12C (como MRTX1133 o Sotorasib) en el adenocarcinoma "
            "ductal pancreático?"
        ),
    },
    {
        "id": "q4_brca_olaparib_platino",
        "titulo": "Mutaciones BRCA1/2 y PALB2 (Olaparib y platino)",
        "categoria": "Medicina de precisión",
        "pregunta": (
            "¿Qué opciones terapéuticas y ensayos clínicos existen para pacientes con "
            "cáncer de páncreas con mutaciones en BRCA1/2 o PALB2 (como Olaparib y "
            "derivados del platino)?"
        ),
    },
    {
        "id": "q5_biomarcadores_ca199_msi",
        "titulo": "Biomarcadores CA19-9 y MSI-H/dMMR e inmunoterapia",
        "categoria": "Biomarcadores e inmunoterapia",
        "pregunta": (
            "¿Qué papel tienen los biomarcadores CA19-9 y MSI-H/dMMR en el "
            "adenocarcinoma ductal pancreático y cómo se relaciona Pembrolizumab con ellos?"
        ),
    },
]

RELATION_TEMPLATES: dict[str, str] = {
    "TREATS": "trata/indica",
    "COMBINES_WITH": "se combina con",
    "TARGETS": "tiene como diana molecular",
    "CAUSES": "causa el evento adverso",
    "CONTRAINDICATED_FOR": "está contraindicado en",
    "BIOMARKER_FOR": "es biomarcador de",
    "DIAGNOSES": "diagnostica",
    "EVALUATED_IN": "es evaluado en el ensayo clínico",
    "HAS_MUTATION": "presenta mutación en",
    "ASSOCIATED_WITH": "está asociado con",
    "RISK_FACTOR_FOR": "es factor de riesgo para",
}

CLINICAL_SYSTEM_PROMPT = f"""Eres OncoPancreaticCDSS, un sistema determinista de apoyo a la decisión clínica en oncología pancreática.

REGLAS ESTRICTAS (CERO ALUCINACIONES):
1. Responde ÚNICA Y EXCLUSIVAMENTE con hechos presentes en el bloque 'CONTEXTO VERIFICADO DEL GRAFO'.
2. PROHIBIDO usar conocimiento externo, inferir dosis, inventar relaciones entre entidades o citar documentos que no aparezcan literalmente en el contexto.
3. Si el contexto no contiene ningún hecho útil para responder a la consulta clínica, responde EXACTAMENTE con la siguiente frase y nada más:
{NO_EVIDENCE_MSG}
4. Redacta la respuesta en español mediante viñetas claras (`-`) y lenguaje clínico natural, resumiendo los hechos verificados de forma concisa y organizada (por ejemplo: tratamientos/regímenes, combinaciones, terapias dirigidas/biomarcadores, ensayos clínicos y eventos adversos).
5. Ten en cuenta las etiquetas de cada entidad:
   - Solo son ensayos clínicos las entidades marcadas como `(TRIAL)` (ej. POLO Trial, NAPOLI-3, NCT05737706, NCT06115499, CodeBreaK 100); NO confundas nombres de documentos (ej. PIIS0923753423008244, TRABAJO DE Cancer Pancreas) con ensayos clínicos.
   - NO confundas pruebas diagnósticas `(PROCEDURE)` como Computed Tomography (CT) con radioterapia.
6. Al final de cada viñeta clínica, cita entre corchetes los documentos exactos que figuran en 'Documentos' (ejemplo: [Fuentes: Article_SEOMClinicalGuidelinesForPancr, NCT06115499])."""


@lru_cache(maxsize=256)
def _call_ollama_cached(
    model: str,
    options_json: str,
    system_text: str,
    prompt: str,
    json_mode: bool,
) -> str:
    """Ejecuta y memoriza de forma determinista llamadas idénticas a Ollama."""
    payload: dict[str, Any] = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": json.loads(options_json),
    }
    if system_text:
        payload["system"] = system_text
    if json_mode:
        payload["format"] = "json"

    response = requests.post(OLLAMA_URL, json=payload, timeout=120)
    response.raise_for_status()
    return response.json().get("response", "").strip()


def call_ollama(
    prompt: str,
    system: str | None = None,
    json_mode: bool = False,
) -> str:
    """Envía un prompt a la instancia local de Ollama con parámetros deterministas."""
    options_json = json.dumps(OLLAMA_OPTIONS, sort_keys=True)
    return _call_ollama_cached(
        OLLAMA_MODEL,
        options_json,
        system or "",
        prompt,
        json_mode,
    )


def classify_domain(question: str) -> dict[str, Any]:
    """Valida si la pregunta pertenece al dominio de oncología pancreática / OncoPancreaticCDSS."""
    q_clean = (question or "").strip()
    if not q_clean:
        return {"in_domain": False, "reason": "La consulta está vacía."}

    classifier_system = (
        "Eres un clasificador estricto de dominio médico para el sistema OncoPancreaticCDSS. "
        "Responde siempre con un único objeto JSON válido."
    )
    classifier_prompt = f"""Determina si la siguiente consulta pertenece al dominio permitido de OncoPancreaticCDSS.

DOMINIO PERMITIDO (in_domain = true):
- Cáncer de páncreas / adenocarcinoma ductal pancreático (PDAC) y neoplasias pancreatobiliares asociadas.
- Fármacos, quimioterapias, terapias dirigidas, inmunoterapia o regímenes oncológicos usados o evaluados en cáncer de páncreas (ej. Gemcitabine, Nab-Paclitaxel, FOLFIRINOX, NALIRIFOX, Olaparib, MRTX1133, Sotorasib, Pembrolizumab, Oxaliplatin, Irinotecan, Cisplatin, Capecitabine, 5-Fluorouracil).
- Biomarcadores, genes y mutaciones pancreáticas (ej. KRAS, BRCA1/2, PALB2, CA19-9, MSI-H/dMMR, TP53).
- Ensayos clínicos, toxicidades, efectos adversos, contraindicaciones, diagnóstico o procedimientos en oncología pancreática (ej. Whipple, CT, radioterapia, ensayos NCT/POLO/NAPOLI-3).
- Consultas sobre el funcionamiento o conocimiento del proyecto OncoPancreaticCDSS.

FUERA DE DOMINIO (in_domain = false):
- Recetas de cocina, gastronomía, deportes, clima/meteorología, programación general, finanzas, historia, viajes, entretenimiento o medicina no relacionada con oncología ni con los fármacos/biomarcadores anteriores.

Devuelve ÚNICAMENTE un JSON con este formato exacto:
{{"in_domain": true, "reason": "Explicación breve en español"}}

Consulta: "{q_clean}"
"""
    try:
        raw_json = call_ollama(classifier_prompt, system=classifier_system, json_mode=True)
        parsed = json.loads(strip_json_fences(raw_json))
        in_domain = parsed.get("in_domain")
        if isinstance(in_domain, bool):
            reason = str(parsed.get("reason") or "").strip() or (
                "Consulta dentro del dominio de oncología pancreática."
                if in_domain
                else "Tema ajeno a oncología pancreática."
            )
            return {"in_domain": in_domain, "reason": reason}
    except (json.JSONDecodeError, requests.RequestException, KeyError, TypeError):
        pass

    return fallback_domain_check(q_clean)


def extract_entities_from_question(question: str) -> list[str]:
    """Extrae términos biomédicos normalizados al inglés de forma determinista."""
    alias_entities = extract_alias_entities(question)

    prompt = f"""Extract ONLY the biomedical entities (drugs, regimens, genes, mutations, diseases, biomarkers, clinical trials, procedures) EXPLICITLY mentioned in the question below.
Translate Spanish terms to standard English medical names (e.g., "cáncer de páncreas" or "adenocarcinoma ductal pancreático" -> "Pancreatic Ductal Adenocarcinoma").
Keep any specific, code-named, or unknown drug/trial names verbatim exactly as written in the question.
Do NOT invent or add entities that are not mentioned in the question.
Return ONLY a valid JSON object with a single key "entities" containing a list of plain strings.

Question: "{question}"
"""
    llm_entities: list[str] = []
    try:
        raw_json = call_ollama(prompt, json_mode=True)
        data = json.loads(strip_json_fences(raw_json))
        raw_list = data.get("entities", [])
        if isinstance(raw_list, list):
            for item in raw_list:
                if isinstance(item, str) and item.strip():
                    llm_entities.append(item.strip())
                elif isinstance(item, dict):
                    for val in item.values():
                        if isinstance(val, str):
                            clean_val = val.strip()
                            if clean_val and clean_val.lower() not in IGNORED_ENTITY_TYPES:
                                llm_entities.append(clean_val)
    except (json.JSONDecodeError, requests.RequestException, TypeError):
        llm_entities = []

    return filter_llm_entities(llm_entities, alias_entities)


@lru_cache(maxsize=64)
def _load_interim_document(category: str, doc_id: str) -> tuple[str, str, str]:
    """Lee el archivo Markdown en data/02_interim y devuelve (título, texto, ruta relativa)."""
    if not category or not doc_id:
        return doc_id, "", ""

    file_path = INTERIM_DATA_DIR / category / f"{doc_id}.md"
    if not file_path.exists():
        return doc_id, "", ""

    try:
        content = file_path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return doc_id, "", ""

    title = doc_id
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            title = stripped.lstrip("#").strip()
            break

    try:
        rel_path = str(file_path.relative_to(BASE_DIR)).replace("\\", "/")
    except ValueError:
        rel_path = str(file_path).replace("\\", "/")

    return title, content, rel_path


def _extract_relevant_snippet(content: str, search_terms: list[str], max_len: int = 320) -> str:
    """Extrae un fragmento textual relevante del documento asociado a las entidades."""
    if not content:
        return "Fragmento de texto no disponible en el repositorio local."

    paragraphs = [
        re.sub(r"\s+", " ", p.strip())
        for p in re.split(r"\n\s*\n", content)
        if p.strip() and not p.strip().startswith("#") and len(p.strip()) > 40
    ]
    if not paragraphs:
        compact = re.sub(r"\s+", " ", content).strip()
        return compact[:max_len] + ("..." if len(compact) > max_len else "")

    terms_lower = [t.lower() for t in search_terms if len(t) >= 3]
    best_para = paragraphs[0]
    best_hits = -1
    for para in paragraphs:
        para_lower = para.lower()
        hits = sum(1 for term in terms_lower if term in para_lower)
        if hits > best_hits:
            best_hits = hits
            best_para = para

    if len(best_para) <= max_len:
        return best_para
    return best_para[: max_len - 3].rstrip() + "..."


def _parse_source_file(source_path: str) -> tuple[str, str]:
    """Convierte la ruta de Databricks en (categoría, doc_id limpio)."""
    parts = [p for p in source_path.replace("\\", "/").split("/") if p]
    category = parts[-2] if len(parts) >= 2 else ""
    filename = urllib.parse.unquote(parts[-1]) if parts else source_path
    doc_id = filename.replace("_triplets.json", "").replace(".json", "")
    return category, doc_id


def query_neo4j_subgraph(
    driver: Any,
    entities: list[str],
    preferred_rels: list[str] | None = None,
    limit: int = 25,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    """Consulta Neo4j de forma determinista y devuelve (tripletas, documentos, texto_contexto)."""
    if not entities:
        return [], [], ""

    query_terms = select_query_terms(entities)
    high_prio_rels, med_prio_rels = split_priority_relations(preferred_rels)

    cypher = """
    MATCH (s:Entity)-[r]->(t:Entity)
    WHERE ANY(term IN $terms WHERE toLower(s.name) CONTAINS toLower(term) OR toLower(t.name) CONTAINS toLower(term))
    WITH s, r, t,
         size([term IN $terms WHERE toLower(s.name) CONTAINS toLower(term) OR toLower(t.name) CONTAINS toLower(term)]) AS match_score,
         CASE
           WHEN type(r) IN $high_prio_rels THEN 2
           WHEN type(r) IN $med_prio_rels THEN 1
           ELSE 0
         END AS rel_prio
    RETURN DISTINCT
        s.name AS sujeto,
        coalesce(s.label, 'ENTITY') AS tipo_sujeto,
        type(r) AS relacion,
        t.name AS objeto,
        coalesce(t.label, 'ENTITY') AS tipo_objeto,
        coalesce(r.evidence_count, 1) AS evidencias,
        coalesce(r.doc_count, 1) AS doc_count,
        coalesce(r.avg_confidence, 0.0) AS confianza,
        r.source_files AS fuentes,
        match_score,
        rel_prio
    ORDER BY rel_prio DESC, evidencias DESC, doc_count DESC, match_score DESC, sujeto ASC, relacion ASC, objeto ASC
    LIMIT $fetch_limit
    """

    tripletas: list[dict[str, Any]] = []
    context_lines: list[str] = []
    doc_registry: dict[str, dict[str, Any]] = {}
    seen_pairs: set[tuple[frozenset[str], str]] = set()

    with driver.session(database=NEO4J_DB) as session:
        records = session.run(
            cypher,
            terms=query_terms,
            high_prio_rels=high_prio_rels,
            med_prio_rels=med_prio_rels,
            fetch_limit=limit * 2,
        )

        for rec in records:
            pair_key = (frozenset([rec["sujeto"], rec["objeto"]]), rec["relacion"])
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)

            raw_sources = sorted(rec["fuentes"] or [])
            clean_doc_ids: list[str] = []
            for sf in raw_sources:
                category, doc_id = _parse_source_file(sf)
                if doc_id not in clean_doc_ids:
                    clean_doc_ids.append(doc_id)

                if doc_id not in doc_registry:
                    title, content, rel_path = _load_interim_document(category, doc_id)
                    doc_registry[doc_id] = {
                        "id": doc_id,
                        "titulo": title,
                        "categoria": CATEGORY_LABELS.get(category, category or "Documento"),
                        "categoria_raw": category,
                        "ruta": rel_path,
                        "tripletas_asociadas": 0,
                        "entidades_relacionadas": set(),
                        "_content": content,
                    }
                doc_registry[doc_id]["tripletas_asociadas"] += 1
                doc_registry[doc_id]["entidades_relacionadas"].update(
                    [rec["sujeto"], rec["objeto"]]
                )

            fuentes_str = ", ".join(clean_doc_ids[:4]) if clean_doc_ids else "Sin fuente"
            formato = f"({rec['sujeto']}) -> [{rec['relacion']}] -> ({rec['objeto']})"

            triplet_item = {
                "sujeto": rec["sujeto"],
                "tipo_sujeto": rec["tipo_sujeto"],
                "relacion": rec["relacion"],
                "objeto": rec["objeto"],
                "tipo_objeto": rec["tipo_objeto"],
                "evidencias": int(rec["evidencias"]),
                "doc_count": int(rec["doc_count"]),
                "confianza": round(float(rec["confianza"]), 3),
                "fuentes": clean_doc_ids,
                "formato": formato,
            }
            tripletas.append(triplet_item)

            rel_phrase = RELATION_TEMPLATES.get(rec["relacion"], rec["relacion"].lower().replace("_", " "))
            context_lines.append(
                f"{len(tripletas)}. {rec['sujeto']} ({rec['tipo_sujeto']}) {rel_phrase} "
                f"{rec['objeto']} ({rec['tipo_objeto']}) "
                f"[Evidencias: {triplet_item['evidencias']} | Documentos: {fuentes_str}]"
            )

            if len(tripletas) >= limit:
                break

    documentos: list[dict[str, Any]] = []
    for doc_info in doc_registry.values():
        entities_for_doc = sorted(doc_info["entidades_relacionadas"])
        snippet = _extract_relevant_snippet(doc_info["_content"], entities_for_doc + query_terms)
        documentos.append(
            {
                "id": doc_info["id"],
                "titulo": doc_info["titulo"],
                "categoria": doc_info["categoria"],
                "ruta": doc_info["ruta"],
                "tripletas_asociadas": doc_info["tripletas_asociadas"],
                "fragmento": snippet,
            }
        )

    documentos.sort(key=lambda d: (-d["tripletas_asociadas"], d["id"].lower()))
    return tripletas, documentos, "\n".join(context_lines)


def get_graph_stats() -> dict[str, Any]:
    """Obtiene métricas globales del grafo Neo4j para monitorización y UI."""
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        with driver.session(database=NEO4J_DB) as session:
            nodos = session.run("MATCH (n:Entity) RETURN count(n) AS c").single()["c"]
            relaciones = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
            docs = session.run(
                "MATCH ()-[r]->() UNWIND r.source_files AS sf RETURN count(DISTINCT sf) AS c"
            ).single()["c"]
            top_relaciones = [
                {"tipo": rec["t"], "total": rec["c"]}
                for rec in session.run(
                    "MATCH ()-[r]->() RETURN type(r) AS t, count(r) AS c ORDER BY c DESC"
                )
            ]
        return {
            "nodos": int(nodos),
            "relaciones": int(relaciones),
            "documentos": int(docs),
            "tipos_relacion": top_relaciones,
        }
    finally:
        driver.close()


def ask_graphrag(
    question: str,
    limit: int = 25,
    verbose: bool = False,
) -> dict[str, Any]:
    """Orquesta el pipeline completo GraphRAG: Guardrail -> Entidades -> Neo4j -> Ollama."""
    q_clean = (question or "").strip()

    # 1. Filtro estricto de dominio
    domain_check = classify_domain(q_clean)
    if not domain_check["in_domain"]:
        if verbose:
            print(f"[BLOQUEADA] {domain_check['reason']}")
        return {
            "pregunta": q_clean,
            "en_dominio": False,
            "motivo_filtro": domain_check["reason"],
            "entidades": [],
            "respuesta": OUT_OF_DOMAIN_MSG,
            "tripletas": [],
            "documentos": [],
            "sin_evidencia": True,
        }

    # 2. Extracción determinista de entidades e intención
    entities = extract_entities_from_question(q_clean)
    preferred_rels = detect_preferred_relations(q_clean)

    if verbose:
        print(f"Pregunta: {q_clean}")
        print(f"1. Entidades detectadas: {entities}")
        print(f"   Relaciones prioritarias: {preferred_rels}")

    if not entities:
        return {
            "pregunta": q_clean,
            "en_dominio": True,
            "motivo_filtro": domain_check["reason"],
            "entidades": [],
            "respuesta": NO_EVIDENCE_MSG,
            "tripletas": [],
            "documentos": [],
            "sin_evidencia": True,
        }

    # 3. Recuperación de subgrafo y trazabilidad documental en Neo4j
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        tripletas, documentos, context = query_neo4j_subgraph(
            driver=driver,
            entities=entities,
            preferred_rels=preferred_rels,
            limit=limit,
        )
    finally:
        driver.close()

    if not tripletas or not context:
        if verbose:
            print("2. Neo4j no devolvió tripletas para las entidades extraídas.")
        return {
            "pregunta": q_clean,
            "en_dominio": True,
            "motivo_filtro": domain_check["reason"],
            "entidades": entities,
            "respuesta": NO_EVIDENCE_MSG,
            "tripletas": [],
            "documentos": [],
            "sin_evidencia": True,
        }

    if verbose:
        print(f"2. Tripletas recuperadas ({len(tripletas)}):\n{context}\n")

    # 4. Síntesis clínica estrictamente anclada al grafo
    user_prompt = f"""### CONTEXTO VERIFICADO DEL GRAFO:
{context}

### PREGUNTA CLÍNICA:
{q_clean}

### RESPUESTA CLÍNICA ESTRUCTURADA (en español, solo hechos del contexto, en viñetas clínicas claras y con citas [Fuentes: ...]):"""

    if verbose:
        print("3. Generando síntesis clínica determinista con Ollama...")

    answer = call_ollama(user_prompt, system=CLINICAL_SYSTEM_PROMPT, json_mode=False)
    sin_evidencia = NO_EVIDENCE_MSG.lower() in answer.lower()

    return {
        "pregunta": q_clean,
        "en_dominio": True,
        "motivo_filtro": domain_check["reason"],
        "entidades": entities,
        "respuesta": answer,
        "tripletas": tripletas,
        "documentos": documentos,
        "sin_evidencia": sin_evidencia,
    }


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    sample_question = STAR_CLINICAL_QUESTIONS[0]["pregunta"]
    result = ask_graphrag(sample_question, verbose=True)
    print("\n================ RESPUESTA ================")
    print(result["respuesta"])
    print(f"\nTripletas: {len(result['tripletas'])} | Documentos: {len(result['documentos'])}")
    print("===========================================")
