from __future__ import annotations

import html
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

APP_DIR = Path(__file__).resolve().parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from ScriptOllama import (  # noqa: E402
    OLLAMA_MODEL,
    OLLAMA_OPTIONS,
    STAR_CLINICAL_QUESTIONS,
    ask_graphrag,
    get_graph_stats,
)

# ==========================================
# CONFIGURACIÓN DE PÁGINA Y ESTILOS
# ==========================================
st.set_page_config(
    page_title="OncoPancreaticCDSS | GraphRAG Clínico",
    layout="wide",
    initial_sidebar_state="expanded",
)

LABEL_COLORS: dict[str, tuple[str, str]] = {
    "DRUG": ("#DBEAFE", "#1E40AF"),
    "REGIMEN": ("#E0E7FF", "#3730A3"),
    "DISEASE": ("#FEE2E2", "#991B1B"),
    "MUTATION": ("#FCE7F3", "#9D174D"),
    "GENE": ("#F3E8FF", "#6B21A8"),
    "BIOMARKER": ("#CCFBF1", "#115E59"),
    "TRIAL": ("#FEF3C7", "#92400E"),
    "ADVERSE_EVENT": ("#FFEDD5", "#9A3412"),
    "PROCEDURE": ("#E2E8F0", "#334155"),
    "STAGE": ("#E0F2FE", "#075985"),
}

CATEGORY_COLORS: dict[str, tuple[str, str]] = {
    "Guía Clínica": ("#DCFCE7", "#166534"),
    "Artículo Científico": ("#DBEAFE", "#1E40AF"),
    "Ensayo Clínico": ("#FEF3C7", "#92400E"),
    "Ficha Técnica de Fármaco": ("#F3E8FF", "#6B21A8"),
}

st.markdown(
    """
<style>
.cdss-header {
    background: linear-gradient(135deg, #0F172A 0%, #1E3A5F 100%);
    padding: 1.35rem 1.6rem;
    border-radius: 12px;
    color: #F8FAFC;
    margin-bottom: 1.1rem;
    border: 1px solid #334155;
}
.cdss-header h1 {
    margin: 0;
    font-size: 1.75rem;
    font-weight: 700;
    color: #FFFFFF;
}
.cdss-header p {
    margin: 0.35rem 0 0 0;
    color: #CBD5E1;
    font-size: 0.95rem;
}
.cdss-card {
    background-color: #FFFFFF;
    color: #0F172A;
    border: 1px solid #E2E8F0;
    border-left: 5px solid #0284C7;
    border-radius: 10px;
    padding: 1.2rem 1.4rem;
    margin-bottom: 1rem;
    box-shadow: 0 1px 3px rgba(15, 23, 42, 0.06);
}
.triplet-row {
    background-color: #F8FAFC;
    color: #0F172A;
    border: 1px solid #CBD5E1;
    border-radius: 8px;
    padding: 0.65rem 0.9rem;
    margin-bottom: 0.5rem;
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: 0.5rem;
}
.triplet-main {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.4rem;
    font-size: 0.92rem;
    color: #0F172A;
}
.triplet-main strong {
    color: #0F172A;
}
.triplet-arrow {
    color: #475569;
    font-weight: 700;
}
.badge {
    display: inline-block;
    padding: 0.16rem 0.52rem;
    border-radius: 999px;
    font-size: 0.76rem;
    font-weight: 600;
    letter-spacing: 0.01em;
}
.rel-pill {
    background-color: #0F172A;
    color: #F8FAFC;
    padding: 0.16rem 0.55rem;
    border-radius: 6px;
    font-size: 0.75rem;
    font-weight: 600;
    font-family: monospace;
}
.triplet-meta {
    color: #334155;
    font-size: 0.8rem;
    white-space: nowrap;
}
.triplet-meta strong {
    color: #0F172A;
}
.entity-chip {
    display: inline-block;
    background-color: #EFF6FF;
    color: #1D4ED8;
    border: 1px solid #BFDBFE;
    border-radius: 999px;
    padding: 0.18rem 0.6rem;
    font-size: 0.8rem;
    font-weight: 600;
    margin-right: 0.35rem;
    margin-bottom: 0.35rem;
}
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_data(ttl=300, show_spinner=False)
def cached_graph_stats() -> dict:
    try:
        return get_graph_stats()
    except Exception as exc:
        return {"error": str(exc)}


def _badge_html(text: str, palette: dict[str, tuple[str, str]], default: tuple[str, str] = ("#F1F5F9", "#334155")) -> str:
    bg, fg = palette.get(text, default)
    return f'<span class="badge" style="background:{bg};color:{fg};">{html.escape(text)}</span>'


def _set_star_question(question_text: str) -> None:
    st.session_state["query_input"] = question_text
    st.session_state["run_query_text"] = question_text


def _clear_query() -> None:
    st.session_state["query_input"] = ""
    st.session_state["run_query_text"] = None
    st.session_state["last_result"] = None


# Inicialización de estado de sesión
if "query_input" not in st.session_state:
    st.session_state["query_input"] = ""
if "run_query_text" not in st.session_state:
    st.session_state["run_query_text"] = None
if "last_result" not in st.session_state:
    st.session_state["last_result"] = None
if "history" not in st.session_state:
    st.session_state["history"] = []


# ==========================================
# BARRA LATERAL (MÉTRICAS, PARÁMETROS E HISTORIAL)
# ==========================================
with st.sidebar:
    st.markdown("## OncoPancreaticCDSS")
    st.caption(
        "Sistema de Apoyo a la Decisión Clínica basado en Grafo de Conocimiento (Neo4j) "
        "y síntesis determinista con Ollama."
    )
    st.divider()

    stats = cached_graph_stats()
    if "error" in stats:
        st.warning(f"No se pudo conectar a Neo4j: {stats['error']}")
    else:
        st.markdown("### Estado del Grafo Neo4j")
        c1, c2 = st.columns(2)
        c1.metric("Nodos", f"{stats['nodos']:,}")
        c2.metric("Relaciones", f"{stats['relaciones']:,}")
        st.metric("Documentos Fuente", f"{stats['documentos']}")

    st.divider()
    st.markdown("### Configuración Determinista")
    st.code(
        f"Modelo: {OLLAMA_MODEL}\n"
        f"temperature: {OLLAMA_OPTIONS['temperature']}\n"
        f"top_p: {OLLAMA_OPTIONS['top_p']}\n"
        f"repeat_penalty: {OLLAMA_OPTIONS['repeat_penalty']}\n"
        f"seed: {OLLAMA_OPTIONS['seed']}",
        language="yaml",
    )

    triplet_limit = st.slider(
        "Máximo de tripletas a recuperar",
        min_value=10,
        max_value=40,
        value=25,
        step=5,
        help="Controla cuántas relaciones verificadas de Neo4j se envían al contexto clínico.",
    )

    if st.session_state["history"]:
        st.divider()
        st.markdown("### Historial Reciente")
        for idx, entry in enumerate(reversed(st.session_state["history"][-5:])):
            label = entry["pregunta"][:56] + ("..." if len(entry["pregunta"]) > 56 else "")
            if st.button(label, key=f"hist_{idx}", width="stretch"):
                st.session_state["last_result"] = entry

    st.divider()
    st.caption(
        "Uso exclusivo de apoyo informativo y trazabilidad bibliográfica. "
        "No sustituye el criterio del comité multidisciplinar ni la evaluación médica directa."
    )


# ==========================================
# CABECERA PRINCIPAL
# ==========================================
st.markdown(
    """
<div class="cdss-header">
    <h1>OncoPancreaticCDSS — GraphRAG Clínico en Cáncer de Páncreas</h1>
    <p>Respuestas clínicas deterministas, estrictamente ancladas a tripletas verificadas en Neo4j y documentos médicos trazables.</p>
</div>
""",
    unsafe_allow_html=True,
)


# ==========================================
# SELECTOR DE LAS 5 PREGUNTAS CLÍNICAS ESTRELLA
# ==========================================
st.markdown("### Consultas Clínicas Estrella (Validadas)")
st.caption("Acceso rápido a consultas representativas sobre combinaciones, regímenes, mutaciones y biomarcadores del grafo:")

row1 = st.columns(3)
for idx, item in enumerate(STAR_CLINICAL_QUESTIONS[:3]):
    with row1[idx]:
        st.caption(f"**{item['categoria']}**")
        st.button(
            item["titulo"],
            key=f"star_btn_{item['id']}",
            width="stretch",
            on_click=_set_star_question,
            args=(item["pregunta"],),
        )

row2 = st.columns(2)
for idx, item in enumerate(STAR_CLINICAL_QUESTIONS[3:]):
    with row2[idx]:
        st.caption(f"**{item['categoria']}**")
        st.button(
            item["titulo"],
            key=f"star_btn_{item['id']}",
            width="stretch",
            on_click=_set_star_question,
            args=(item["pregunta"],),
        )


# ==========================================
# ENTRADA LIBRE DEL MÉDICO / USUARIO
# ==========================================
st.markdown("### Consulta Clínica Personalizada")
query_text = st.text_area(
    "Introduce tu consulta sobre oncología pancreática (fármacos, mutaciones, biomarcadores, toxicidades o ensayos clínicos):",
    key="query_input",
    height=95,
    placeholder="Ejemplo: ¿Qué toxicidades provoca Oxaliplatin o qué opciones existen para pacientes con mutación BRCA1/2?",
)

col_submit, col_clear, _ = st.columns([2, 1, 5])
with col_submit:
    submit_clicked = st.button("Consultar OncoPancreaticCDSS", type="primary", width="stretch")
with col_clear:
    st.button("Limpiar", width="stretch", on_click=_clear_query)

if submit_clicked:
    st.session_state["run_query_text"] = query_text

if st.session_state["run_query_text"] is not None:
    active_query = st.session_state["run_query_text"].strip()
    st.session_state["run_query_text"] = None
    if not active_query:
        st.warning("Por favor, introduce una consulta clínica antes de ejecutar.")
    else:
        with st.spinner("Validando dominio, consultando Neo4j y sintetizando evidencia clínica..."):
            try:
                res = ask_graphrag(active_query, limit=triplet_limit)
                st.session_state["last_result"] = res
                st.session_state["history"].append(res)
            except Exception as exc:
                st.error(f"Error al ejecutar la consulta GraphRAG (verifica que Neo4j y Ollama estén activos): {exc}")


# ==========================================
# PANEL DE RESULTADOS Y EXPLICABILIDAD (XAI)
# ==========================================
result = st.session_state.get("last_result")

if result:
    st.divider()

    if not result["en_dominio"]:
        st.error(
            f"**Consulta bloqueada por el Guardrail de Dominio**\n\n"
            f"{result['respuesta']}\n\n"
            f"**Motivo del clasificador:** {result['motivo_filtro']}"
        )
    elif result["sin_evidencia"]:
        st.warning(
            f"**Sin evidencia suficiente en el grafo de conocimiento**\n\n"
            f"{result['respuesta']}"
        )
        if result["entidades"]:
            chips = "".join(f'<span class="entity-chip">{html.escape(e)}</span>' for e in result["entidades"])
            st.markdown(f"**Entidades buscadas:** {chips}", unsafe_allow_html=True)
    else:
        # Métricas de trazabilidad
        m1, m2, m3 = st.columns(3)
        m1.metric("Entidades detectadas", len(result["entidades"]))
        m2.metric("Tripletas recuperadas", len(result["tripletas"]))
        m3.metric("Documentos fuente", len(result["documentos"]))

        if result["entidades"]:
            chips = "".join(f'<span class="entity-chip">{html.escape(e)}</span>' for e in result["entidades"])
            st.markdown(f"**Entidades clínicas mapeadas:** {chips}", unsafe_allow_html=True)

        # Panel Principal: Síntesis Clínica
        st.markdown("### Síntesis Clínica (Basada Exclusivamente en Evidencia del Grafo)")
        with st.container(border=True):
            st.markdown(result["respuesta"])

        # Panel de Evidencia y Explicabilidad (XAI)
        st.markdown("### Panel de Evidencia y Explicabilidad (XAI)")
        tab_triplets, tab_docs = st.tabs(
            [
                f"Tripletas del Grafo ({len(result['tripletas'])})",
                f"Documentos Fuente ({len(result['documentos'])})",
            ]
        )

        with tab_triplets:
            st.caption("Relaciones exactas recuperadas de Neo4j que fundamentan la síntesis clínica:")
            for t in result["tripletas"]:
                s_badge = _badge_html(t["tipo_sujeto"], LABEL_COLORS)
                o_badge = _badge_html(t["tipo_objeto"], LABEL_COLORS)
                row_html = (
                    f'<div class="triplet-row">'
                    f'  <div class="triplet-main">'
                    f'    <strong>{html.escape(t["sujeto"])}</strong> {s_badge}'
                    f'    <span class="triplet-arrow">&rarr;</span>'
                    f'    <span class="rel-pill">{html.escape(t["relacion"])}</span>'
                    f'    <span class="triplet-arrow">&rarr;</span>'
                    f'    <strong>{html.escape(t["objeto"])}</strong> {o_badge}'
                    f'  </div>'
                    f'  <div class="triplet-meta">'
                    f'    Evidencias: <strong>{t["evidencias"]}</strong> | '
                    f'    Docs: <strong>{t["doc_count"]}</strong> | '
                    f'    Confianza: <strong>{t["confianza"]}</strong>'
                    f'  </div>'
                    f'</div>'
                )
                st.markdown(row_html, unsafe_allow_html=True)

            with st.expander("Ver tabla estructurada de tripletas (filtrable y exportable)"):
                df_triplets = pd.DataFrame(
                    [
                        {
                            "Sujeto": t["sujeto"],
                            "Tipo Sujeto": t["tipo_sujeto"],
                            "Relación": t["relacion"],
                            "Objeto": t["objeto"],
                            "Tipo Objeto": t["tipo_objeto"],
                            "Evidencias": t["evidencias"],
                            "Docs": t["doc_count"],
                            "Confianza": t["confianza"],
                            "Fuentes": ", ".join(t["fuentes"]),
                        }
                        for t in result["tripletas"]
                    ]
                )
                st.dataframe(df_triplets, width="stretch", hide_index=True)

        with tab_docs:
            st.caption("Documentos de respaldo asociados a las tripletas recuperadas:")
            for idx, doc in enumerate(result["documentos"]):
                expander_label = (
                    f"[{doc['categoria']}] {doc['titulo']} "
                    f"— {doc['tripletas_asociadas']} tripleta(s) respaldada(s)"
                )
                with st.expander(expander_label, expanded=(idx < 3)):
                    cat_badge = _badge_html(doc["categoria"], CATEGORY_COLORS)
                    st.markdown(cat_badge, unsafe_allow_html=True)
                    st.markdown(f"**ID del documento:** `{doc['id']}`")
                    if doc["ruta"]:
                        st.markdown(f"**Archivo local:** `{doc['ruta']}`")
                    st.markdown(f"**Fragmento relevante:**\n> {doc['fragmento']}")
