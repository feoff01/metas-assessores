# app.py
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response, FileResponse
from pydantic import BaseModel
from typing import Dict, List, Optional, Set, Tuple
import psycopg2
from psycopg2 import sql
import re
from collections import defaultdict
import csv
import io
import random
import os

# ========== CREDENCIAIS DO POSTGRES ==========
# No Railway, configure em Settings → Variables:
# DB_HOST, DB_PORT=5432, DB_NAME, DB_USER, DB_PASS, DB_SSLMODE=require
DB_HOST = os.getenv("DB_HOST", "bdbarsi.clquwys0y8y8.sa-east-1.rds.amazonaws.com")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "Barsi_admin")
DB_PASS = os.getenv("DB_PASS", "*4482Barsi")  # Recomendo mover para variável e remover do código público
DB_SSLMODE = os.getenv("DB_SSLMODE", "require")

SCHEMA = "public"
TBL_REGEX = re.compile(
    r"^relatorio_positivador_(janeiro|fevereiro|mar[cç]o|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)_2025$",
    re.IGNORECASE
)

MESES_MAP = {
    "janeiro": "2025-01", "fevereiro": "2025-02",
    "marco": "2025-03", "março": "2025-03",
    "abril": "2025-04", "maio": "2025-05", "junho": "2025-06",
    "julho": "2025-07", "agosto": "2025-08", "setembro": "2025-09",
    "outubro": "2025-10", "novembro": "2025-11", "dezembro": "2025-12",
}
MESES_LABEL = {
    "2025-01":"Jan","2025-02":"Fev","2025-03":"Mar","2025-04":"Abr",
    "2025-05":"Mai","2025-06":"Jun","2025-07":"Jul","2025-08":"Ago",
    "2025-09":"Set","2025-10":"Out","2025-11":"Nov","2025-12":"Dez"
}

# ==== Pydantic models ====
class LinhaTabela(BaseModel):
    assessor: str
    mes: str
    ativacoes: int
    captacao: float
    receita: float

class LinhaDetalhe(BaseModel):
    assessor: str
    cliente: str
    ativou_em_m: str
    evadiu_em_m: str
    net_em_m: float
    receita_no_mes: float
    captacao_liquida_em_m: float
    mes: str
    mes_nome: str

class MetasModel(BaseModel):
    ativacoes: Dict[str, int]
    captacao: Dict[str, float]
    receita: Dict[str, float]

class MetricasPayload(BaseModel):
    series: List[str]
    meses: List[str]
    ativacoes: Dict[str, Dict[str, int]]
    captacao: Dict[str, Dict[str, float]]
    receita: Dict[str, Dict[str, float]]
    metas: MetasModel
    tabela: List[LinhaTabela]
    detalhes: List[LinhaDetalhe]

app = FastAPI(title="Positivadores 2025 • Ativações, Captação e Receita")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS, sslmode=DB_SSLMODE
    )

def listar_tabelas_positivador_2025(limit: Optional[int] = None) -> List[str]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = %s
              AND tablename ~ %s
            ORDER BY tablename;
        """, (SCHEMA, r'^relatorio_positivador_.*_2025$'))
        rows = cur.fetchall()
    nomes = [t for (t,) in rows if TBL_REGEX.match(t)]
    return nomes[:limit] if limit else nomes

def mes_from_table(tname: str) -> str:
    m = TBL_REGEX.match(tname)
    if not m:
        return "2025-00"
    token = m.group(1).lower().replace("ç", "c")
    return MESES_MAP.get(token, "2025-00")

# Colunas candidatas
COL_ATIV_CAND    = ["ativou_em_m", "Ativou em M?", "ativou_em_mes", "ativou_m"]
COL_EVAD_CAND    = ["evadiu_em_m", "Evadiu em M?", "evadiu_em_mes", "evadiu_m"]
COL_CAPT_CAND    = ["captacao_liquida_em_m", "Captação Líquida em M", "captacao_em_m", "captacao_liq_m"]
COL_RECE_CAND    = ["receita_no_mes", "Receita no Mês", "receita_mes", "receita_em_m"]
COL_NET_CAND     = ["net_em_m", "Net em M", "net_m", "soma_net_em_m"]
COL_CLIENTE_CAND = ["cliente", "id_cliente", "cod_cliente"]

def descobrir_coluna(conn, schema: str, tabela: str, candidatas: List[str], fallback_contains: str) -> str:
    with conn.cursor() as cur:
        cur.execute("""
          SELECT column_name
          FROM information_schema.columns
          WHERE table_schema=%s AND table_name=%s
        """, (schema, tabela))
        cols = {r[0] for r in cur.fetchall()}
    for c in candidatas:
        for col in cols:
            if col.lower() == c.lower():
                return col
    for col in cols:
        if fallback_contains in col.lower():
            return col
    raise RuntimeError(f"Coluna não encontrada em {schema}.{tabela}: {candidatas[0]}")

def _ident(col: str):
    return sql.Identifier(col) if '"' not in col else sql.SQL(f'"{col}"')

def consultar_metricas(conn, schema: str, tabela: str, col_ativ: str, col_capt: str, col_rece: str):
    col_ativ_sql = _ident(col_ativ)
    col_capt_sql = _ident(col_capt)
    col_rece_sql = _ident(col_rece)
    q = sql.SQL("""
        SELECT
            assessor,
            SUM(CASE
                  WHEN {col_ativ} ILIKE 'sim%' THEN 1
                  WHEN {col_ativ}::text IN ('1','true','t') THEN 1
                  ELSE 0
                END) AS ativacoes,
            SUM(COALESCE(NULLIF(REPLACE({col_capt}::text, ',', '.'), '')::numeric,0)) AS captacao,
            SUM(COALESCE(NULLIF(REPLACE({col_rece}::text, ',', '.'), '')::numeric,0)) AS receita
        FROM {sch}.{tb}
        GROUP BY assessor
    """).format(
        col_ativ=col_ativ_sql, col_capt=col_capt_sql, col_rece=col_rece_sql,
        sch=sql.Identifier(schema), tb=sql.Identifier(tabela)
    )
    with conn.cursor() as cur:
        cur.execute(q)
        return cur.fetchall()

def _as_sim_nao(v) -> str:
    if v is None:
        return "Não"
    s = str(v).strip().lower()
    return "Sim" if s in ("1","true","t","sim","s","y","yes") else "Não"

def _as_float(v) -> float:
    if v is None:
        return 0.0
    s = str(v).strip()
    if s == "":
        return 0.0
    s = s.replace(".", "").replace(",", ".") if s.count(",") and s.count(".")>1 else s.replace(",", ".")
    try:
        return float(s)
    except:
        return 0.0

def consultar_detalhes(conn, schema: str, tabela: str,
                       col_assessor: str, col_cliente: str,
                       col_ativ: str, col_evad: str,
                       col_net: str, col_rece: str, col_capt: str) -> List[Tuple]:
    q = sql.SQL("""
        SELECT
            {assessor}::text,
            {cliente}::text,
            {ativ}::text,
            {evad}::text,
            {net},
            {rece},
            {capt}
        FROM {sch}.{tb}
    """).format(
        assessor=_ident(col_assessor), cliente=_ident(col_cliente),
        ativ=_ident(col_ativ), evad=_ident(col_evad),
        net=_ident(col_net), rece=_ident(col_rece), capt=_ident(col_capt),
        sch=sql.Identifier(schema), tb=sql.Identifier(tabela)
    )
    with conn.cursor() as cur:
        cur.execute(q)
        return cur.fetchall()

def _seeded_rng(key: str) -> random.Random:
    h = abs(hash(key)) & 0xFFFFFFFF
    return random.Random(h)

def montar_payload(limit_tables: Optional[int] = None,
                   assessores_filter: Optional[Set[str]] = None) -> MetricasPayload:
    tabelas = listar_tabelas_positivador_2025(limit=limit_tables)

    ativ_matrix = defaultdict(lambda: defaultdict(int))
    capt_matrix = defaultdict(lambda: defaultdict(float))
    rece_matrix = defaultdict(lambda: defaultdict(float))
    linhas: List[LinhaTabela] = []
    detalhes: List[LinhaDetalhe] = []

    with get_conn() as conn:
        for t in tabelas:
            mes = mes_from_table(t)

            # Colunas (agregados)
            col_ativ = descobrir_coluna(conn, SCHEMA, t, COL_ATIV_CAND, "ativ")
            col_capt = descobrir_coluna(conn, SCHEMA, t, COL_CAPT_CAND, "capta")
            col_rece = descobrir_coluna(conn, SCHEMA, t, COL_RECE_CAND, "receit")

            # Métricas agregadas
            for assessor, ativ, capt, rece in consultar_metricas(conn, SCHEMA, t, col_ativ, col_capt, col_rece):
                ass = str(assessor)
                if assessores_filter and ass not in assessores_filter:
                    continue
                a = int(ativ or 0)
                c = float(capt or 0.0)
                r = float(rece or 0.0)
                ativ_matrix[ass][mes] += a
                capt_matrix[ass][mes] += c
                rece_matrix[ass][mes] += r
                linhas.append(LinhaTabela(assessor=ass, mes=mes, ativacoes=a, captacao=c, receita=r))

            # Detalhes (linha a linha)
            col_assessor = "assessor"
            col_cliente  = descobrir_coluna(conn, SCHEMA, t, COL_CLIENTE_CAND, "client")
            col_evad     = descobrir_coluna(conn, SCHEMA, t, COL_EVAD_CAND, "evad")
            col_net      = descobrir_coluna(conn, SCHEMA, t, COL_NET_CAND, "net")

            raw_rows = consultar_detalhes(
                conn, SCHEMA, t,
                col_assessor, col_cliente,
                col_ativ, col_evad,
                col_net, col_rece, col_capt
            )

            for r in raw_rows:
                ass = str(r[0]) if r[0] is not None else ""
                if assessores_filter and ass not in assessores_filter:
                    continue
                cliente = str(r[1]) if r[1] is not None else ""
                ativou  = _as_sim_nao(r[2])
                evadiu  = _as_sim_nao(r[3])
                net     = _as_float(r[4])
                receita = _as_float(r[5])
                capt    = _as_float(r[6])
                detalhes.append(
                    LinhaDetalhe(
                        assessor=ass,
                        cliente=cliente,
                        ativou_em_m=ativou,
                        evadiu_em_m=evadiu,
                        net_em_m=net,
                        receita_no_mes=receita,
                        captacao_liquida_em_m=capt,
                        mes=mes,
                        mes_nome=MESES_LABEL.get(mes, mes)
                    )
                )

    meses_presentes = sorted(
        {m for a in ativ_matrix for m in ativ_matrix[a].keys()} |
        {m for a in capt_matrix for m in capt_matrix[a].keys()} |
        {m for a in rece_matrix for m in rece_matrix[a].keys()}
    )
    assessores = sorted(set(list(ativ_matrix.keys()) + list(capt_matrix.keys()) + list(rece_matrix.keys())))

    ativacoes = {a: {m: int(ativ_matrix[a].get(m, 0)) for m in meses_presentes} for a in assessores}
    captacao  = {a: {m: float(capt_matrix[a].get(m, 0.0)) for m in meses_presentes} for a in assessores}
    receita   = {a: {m: float(rece_matrix[a].get(m, 0.0)) for m in meses_presentes} for a in assessores}

    # Metas simuladas (determinísticas)
    metas_ativ: Dict[str, int] = {}
    metas_capt: Dict[str, float] = {}
    metas_rece: Dict[str, float] = {}
    for a in assessores:
        total_a = sum(ativacoes[a].values())
        total_c = sum(captacao[a].values())
        total_r = sum(receita[a].values())
        rng_a = _seeded_rng(a + "_ativ")
        rng_c = _seeded_rng(a + "_capt")
        rng_r = _seeded_rng(a + "_rece")
        f_a = rng_a.uniform(0.80, 1.40)
        f_c = rng_c.uniform(0.85, 1.35)
        f_r = rng_r.uniform(0.90, 1.30)
        metas_ativ[a] = max(1, int(round(total_a * f_a))) if total_a > 0 else rng_a.randint(2, 10)
        mc = total_c * f_c if total_c > 0 else rng_c.uniform(30000, 300000)
        mr = total_r * f_r if total_r > 0 else rng_r.uniform(20000, 200000)
        metas_capt[a] = float(int(mc // 1000) * 1000)
        metas_rece[a] = float(int(mr // 1000) * 1000)
    metas = MetasModel(ativacoes=metas_ativ, captacao=metas_capt, receita=metas_rece)

    return MetricasPayload(
        series=assessores, meses=meses_presentes,
        ativacoes=ativacoes, captacao=captacao, receita=receita,
        metas=metas, tabela=linhas, detalhes=detalhes
    )

def listar_assessores() -> List[str]:
    asses: Set[str] = set()
    tabelas = listar_tabelas_positivador_2025()
    with get_conn() as conn, conn.cursor() as cur:
        for t in tabelas:
            cur.execute(sql.SQL("""SELECT DISTINCT assessor FROM {sch}.{tb}""")
                       .format(sch=sql.Identifier(SCHEMA), tb=sql.Identifier(t)))
            for (a,) in cur.fetchall():
                asses.add(str(a))
    return sorted(asses, key=lambda x: (len(x), x))

# ================= Endpoints =================
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/api/tabelas", response_model=List[str])
def api_tabelas():
    return listar_tabelas_positivador_2025()

@app.get("/api/assessores", response_model=List[str])
def api_assessores():
    return listar_assessores()

@app.get("/api/metricas", response_model=MetricasPayload)
def api_metricas(
    limit_tables: Optional[int] = Query(default=None, ge=1),
    assessores: Optional[str] = Query(default=None, description="CSV de IDs de assessor")
):
    filtro = None
    if assessores:
        filtro = {s.strip() for s in assessores.split(",") if s.strip()}
    return montar_payload(limit_tables=limit_tables, assessores_filter=filtro)

# Compatibilidade com seu HTML antigo: /api/ativacoes → {ok, rows}
@app.get("/api/ativacoes")
def api_ativacoes(
    limit_tables: Optional[int] = Query(default=None, ge=1),
    assessores: Optional[str] = Query(default=None)
):
    filtro = None
    if assessores:
        filtro = {s.strip() for s in assessores.split(",") if s.strip()}
    payload = montar_payload(limit_tables=limit_tables, assessores_filter=filtro)
    rows = [
        {"assessor": t.assessor, "mes": t.mes, "ativacoes": int(t.ativacoes)}
        for t in payload.tabela
    ]
    return {"ok": True, "rows": rows, "skipped": []}

@app.get("/api/metricas_csv")
def api_metricas_csv(
    limit_tables: Optional[int] = Query(default=None, ge=1),
    assessores: Optional[str] = Query(default=None)
):
    filtro = None
    if assessores:
        filtro = {s.strip() for s in assessores.split(",") if s.strip()}
    payload = montar_payload(limit_tables=limit_tables, assessores_filter=filtro)

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["assessor","cliente","ativou_em_m","evadiu_em_m","net_em_m","receita_no_mes","captacao_liquida_em_m","mes","mes_nome"])
    for d in payload.detalhes:
        w.writerow([d.assessor, d.cliente, d.ativou_em_m, d.evadiu_em_m,
                    f"{d.net_em_m:.2f}", f"{d.receita_no_mes:.2f}",
                    f"{d.captacao_liquida_em_m:.2f}", d.mes, d.mes_nome])
    csv_bytes = buf.getvalue().encode("utf-8")
    return Response(content=csv_bytes, media_type="text/csv",
                    headers={"Content-Disposition": "attachment; filename=detalhes_filtrado.csv"})

# ===== Frontend =====
@app.get("/", response_class=HTMLResponse)
def index():
    # Serve o index.html que está ao lado do app.py
    base_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(base_dir, "index.html")
    return FileResponse(html_path)
