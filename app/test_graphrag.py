import os
import sys
from pathlib import Path

# Asegura salida UTF-8 en consola Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

APP_DIR = Path(__file__).resolve().parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from ScriptOllama import (  # noqa: E402
    NO_EVIDENCE_MSG,
    STAR_CLINICAL_QUESTIONS,
    ask_graphrag,
)


def test_determinism_3x() -> None:
    print("\n" + "=" * 70)
    print("TEST 1: Determinismo estricto (misma pregunta ejecutada 3 veces)")
    print("=" * 70)
    question = STAR_CLINICAL_QUESTIONS[0]["pregunta"]
    print(f"Pregunta: {question}")

    r1 = ask_graphrag(question, limit=25)
    r2 = ask_graphrag(question, limit=25)
    r3 = ask_graphrag(question, limit=25)

    assert r1["en_dominio"] is True, "La pregunta clínica fue bloqueada erróneamente."
    assert not r1["sin_evidencia"], "La pregunta estrella #1 no devolvió evidencia."
    assert r1["tripletas"] == r2["tripletas"] == r3["tripletas"], (
        "Las tripletas recuperadas difieren entre ejecuciones."
    )
    assert r1["documentos"] == r2["documentos"] == r3["documentos"], (
        "Los documentos trazados difieren entre ejecuciones."
    )
    assert r1["respuesta"] == r2["respuesta"] == r3["respuesta"], (
        "La respuesta generada por Ollama no fue idéntica en las 3 ejecuciones."
    )

    print(
        f"[OK] Determinismo verificado (3/3 idénticas) | "
        f"Tripletas: {len(r1['tripletas'])} | Documentos: {len(r1['documentos'])}"
    )


def test_domain_guardrail() -> None:
    print("\n" + "=" * 70)
    print("TEST 2: Filtro de dominio (bloqueo de consultas fuera de contexto)")
    print("=" * 70)
    out_of_domain_queries = [
        "¿Cómo se hace una receta de paella valenciana paso a paso?",
        "Escribe un script en Python para ordenar una lista con quicksort.",
        "¿Qué tiempo y temperatura hará mañana en Madrid?",
    ]

    for q in out_of_domain_queries:
        res = ask_graphrag(q)
        print(f"Consulta: {q}")
        print(f"  -> en_dominio={res['en_dominio']} | motivo={res['motivo_filtro']}")
        assert res["en_dominio"] is False, f"Debió bloquear la pregunta: {q}"
        assert res["tripletas"] == [], "Una pregunta bloqueada no debe devolver tripletas."
        assert res["documentos"] == [], "Una pregunta bloqueada no debe devolver documentos."

    print("[OK] Todas las preguntas fuera de dominio fueron bloqueadas correctamente.")


def test_no_evidence_abstention() -> None:
    print("\n" + "=" * 70)
    print("TEST 3: Abstención estricta ante fármaco ficticio sin evidencia en el grafo")
    print("=" * 70)
    q = "¿Qué eficacia tiene el fármaco ficticio OncoZeta-999 en el cáncer de páncreas?"
    res = ask_graphrag(q)
    print(f"Consulta: {q}")
    print(
        f"  -> en_dominio={res['en_dominio']} | sin_evidencia={res['sin_evidencia']} "
        f"| entidades={res['entidades']}"
    )
    print(f"  -> respuesta={res['respuesta']}")

    assert res["en_dominio"] is True, "La consulta es de oncología pancreática y debía pasar el filtro."
    assert res["sin_evidencia"] is True, "Debió marcar sin_evidencia=True para un fármaco ficticio."
    assert res["respuesta"] == NO_EVIDENCE_MSG, (
        f"Respuesta inesperada: {res['respuesta']}"
    )
    print("[OK] Abstención estricta verificada sin alucinaciones.")


def test_star_clinical_questions() -> None:
    print("\n" + "=" * 70)
    print("TEST 4: Validación de las 5 Preguntas Clínicas Estrella contra Neo4j")
    print("=" * 70)
    for idx, item in enumerate(STAR_CLINICAL_QUESTIONS, start=1):
        res = ask_graphrag(item["pregunta"], limit=25)
        print(f"\n[{idx}/5] {item['titulo']} ({item['categoria']})")
        print(f"  Pregunta: {item['pregunta']}")
        print(f"  Entidades: {res['entidades']}")
        print(
            f"  Tripletas: {len(res['tripletas'])} | Documentos: {len(res['documentos'])} "
            f"| sin_evidencia={res['sin_evidencia']}"
        )

        assert res["en_dominio"] is True, f"Pregunta estrella #{idx} bloqueada."
        assert res["sin_evidencia"] is False, f"Pregunta estrella #{idx} sin evidencia."
        assert len(res["tripletas"]) > 0, f"Pregunta estrella #{idx} sin tripletas."
        assert len(res["documentos"]) > 0, f"Pregunta estrella #{idx} sin documentos."

        preview_triplets = [t["formato"] for t in res["tripletas"][:3]]
        preview_docs = [d["id"] for d in res["documentos"][:3]]
        print(f"  Top tripletas: {preview_triplets}")
        print(f"  Top docs: {preview_docs}")
        print(f"  Respuesta:\n{res['respuesta']}")

    print("\n[OK] Las 5 preguntas estrella respondieron con evidencia verificada del grafo.")


def main() -> None:
    test_determinism_3x()
    test_domain_guardrail()
    test_no_evidence_abstention()
    test_star_clinical_questions()
    print("\nTODOS LOS TESTS DE VALIDACIÓN PASARON CORRECTAMENTE.")


if __name__ == "__main__":
    main()
