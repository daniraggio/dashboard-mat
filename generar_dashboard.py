"""
generar_dashboard.py
Lee los 5 archivos Excel de /data y genera index.html con el dashboard actualizado.
"""

import json
import re
import sys
from pathlib import Path
import pandas as pd

DATA_DIR = Path("data")
OUTPUT   = Path("index.html")

CUIT_NAMES = {
  "30-51688824-1":"P.Eol. Vientos Olavarría","30-52278060-6":"P.Eol. Aluar I",
  "30-52655265-9":"Pampa Energía","30-52671272-9":"LDC Argentina",
  "30-53378967-2":"Genneia","30-55081599-7":"Eol. Buena Ventura",
  "30-62971795-8":"Helios Santa Rosa 2","30-62982706-0":"Agua del Cajón",
  "30-65225424-8":"Costanera","30-65599219-3":"Dock Sud",
  "30-66346111-3":"AES Paraná","30-66523411-4":"Genneia",
  "30-67642119-6":"Hid. Los Nihuiles","30-69224359-1":"Termoandes",
  "30-69470158-9":"Potrerillos","30-70050075-2":"Solar de Los Llanos",
  "30-70083497-9":"Pampa Energía CC","30-70882520-0":"Solar Anchipurac",
  "30-70969733-8":"Vientos Neuquinos I","30-70982458-5":"Solar Victorica",
  "30-70983613-3":"Eol. Diadema 1","30-71120405-5":"FV C.Honda + La Rioja",
  "30-71132206-6":"Genneia","30-71158444-3":"Pampa Energía",
  "30-71242276-5":"Eol. Bicentenario 2","30-71312200-5":"Vientos Neuquinos I",
  "30-71412830-9":"YPF Energía Eléctrica","30-71415512-8":"P.S. Pampa del Infierno",
  "30-71474937-0":"PS V. María del Río Seco","30-71518744-9":"Eol. La Energética",
  "30-71526384-6":"PS Cura Brochero Ampl.","30-71534545-1":"Raizen",
  "30-71536578-9":"PS Amanecer IV","30-71539912-8":"Eol. 3 Picos",
  "30-71556280-0":"Vientos Olavarría","30-71556281-9":"Genoveva+Manq+Ol",
  "30-71559121-5":"Solar Villa Ángela 1","30-71572949-7":"Solar P. Los Llanos",
  "30-71592816-3":"PE San Luis Norte","30-71595899-2":"FV La Cumbre 2",
  "30-71609599-8":"Eol. La Rinconada","30-71609845-8":"FV La Cumbre 2",
  "30-71610373-7":"PS Cura Brochero","30-71610419-9":"PS V. María del Río Seco M.",
  "30-71617892-3":"Eol. Cañadón León","30-71644849-1":"Genneia",
  "30-71729321-1":"PS La Salvación","30-71765521-0":"Solar El Quemado",
  "30-71853263-5":"Genneia","30-71868101-0":"Hid. El Chocón",
  "30-71868196-7":"Hid. Alicurá","30-71869033-8":"Hid. Cerros Colorados",
  "30-71870493-2":"Hid. Piedra del Águila","30-71872506-9":"Solar La Perla",
  "30-71882580-2":"Solar El Quemado","30-71918704-4":"Hid. Las Maderas",
  "30-99902748-9":"EPEC","33-61597477-9":"Céspedes",
  "33-65030549-9":"Central Puerto","33-68735847-9":"Hid. Reyes",
  "33-70726132-9":"Solar Villa Ángela 1","33-71194489-9":"Roca",
  "33-71869886-9":"Genneia"
}

# ── helpers ──────────────────────────────────────────────────────────────────

def extract_code(s):
    if pd.isna(s): return s
    m = re.match(r'\(([^)]+)\)', str(s))
    return m.group(1).strip() if m else str(s).strip()

def parse_price(s):
    if pd.isna(s): return None
    m = re.search(r'(\d+\.?\d*)\s*U\$S', str(s))
    return float(m.group(1)) if m else None

def get_volume(grp):
    grp = grp.sort_values('Fecha desde')
    anual   = grp['Tope anual [MWh]'].dropna()
    mensual = grp['Tope mensual [MWh]'].dropna()
    if len(anual) > 0:
        v = anual.iloc[-1]
        return round(v, 1) if v > 0 else None
    elif len(mensual) > 0:
        v = mensual.iloc[-1]
        return round(v * 12, 1) if v > 0 else None
    return None

def fmt_month(m):
    MESES = ['','Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']
    y, mo = m.split('-')
    return f"{MESES[int(mo)]} {y}"

# ── load data ─────────────────────────────────────────────────────────────────

print("Leyendo archivos Excel...")

contratos   = pd.read_excel(DATA_DIR / "CONTRATOS.xlsx")
generadores = pd.read_excel(DATA_DIR / "Generador_por_conjunto.xlsx")
params      = pd.read_excel(DATA_DIR / "PARAMETROS_VARIABLES.xlsx")
topes       = pd.read_excel(DATA_DIR / "TOPES.xlsx")

today = pd.Timestamp("today").normalize()
print(f"Fecha de hoy: {today.date()}")

# ── tipo recurso ──────────────────────────────────────────────────────────────

generadores['code'] = generadores['CONJUNTO GENERADOR'].apply(extract_code)
gen_tipo = (generadores.dropna(subset=['TIPO RECURSO'])
            .groupby('code')['TIPO RECURSO'].first().reset_index())

contratos['conj_code'] = contratos['Conj. Generador'].str.strip()
df = contratos.merge(gen_tipo, left_on='conj_code', right_on='code', how='left')

# ── precio ────────────────────────────────────────────────────────────────────

precios = params[params['Parámetro'] == 'Precio de la Energía'].copy()
precios['precio_usd'] = precios['Valor'].apply(parse_price)
precios_latest = (precios.sort_values('Inicio')
                  .groupby('Nro contrato').last()[['precio_usd']].reset_index())

df['Nro contrato'] = df['Nro contrato'].astype(str)
precios_latest['Nro contrato'] = precios_latest['Nro contrato'].astype(str)
df = df.merge(precios_latest, on='Nro contrato', how='left')

# ── volumen ───────────────────────────────────────────────────────────────────

topes['Contrato']    = topes['Contrato'].astype(str).str.zfill(6)
topes['Fecha desde'] = pd.to_datetime(topes['Fecha desde'], errors='coerce')
topes['Fecha hasta'] = pd.to_datetime(topes['Fecha hasta'], errors='coerce')

vol = topes.groupby('Contrato').apply(get_volume).reset_index()
vol.columns = ['Nro contrato', 'tope_anual_mwh']
df = df.merge(vol, on='Nro contrato', how='left')

# ── vigentes ──────────────────────────────────────────────────────────────────

df['vigente']    = df['Fin'] >= today
df['fin_str']    = df['Fin'].dt.strftime('%Y-%m-%d')
df['inicio_str'] = df['Inicio'].dt.strftime('%Y-%m-%d')

vigentes = df[df['vigente']].sort_values('Fin').copy()

def safe_float(x):
    try:
        import math
        v = float(x)
        return None if (math.isnan(v) or v <= 0) else round(v, 2)
    except:
        return None

vigentes['precio_usd']    = vigentes['precio_usd'].apply(safe_float)
vigentes['tope_anual_mwh'] = vigentes['tope_anual_mwh'].apply(safe_float)

# ── build table records ───────────────────────────────────────────────────────

cols = ['Nro contrato','Vendedor','Suministro / grupo dem.','Conj. Generador',
        'TIPO RECURSO','inicio_str','fin_str','precio_usd','Tipo contrato','tope_anual_mwh']
records = vigentes[cols].fillna('').to_dict('records')

# fix numeric cols that got stringified by fillna
for r in records:
    for col in ('precio_usd','tope_anual_mwh'):
        v = r[col]
        r[col] = safe_float(v) if v != '' else None

print(f"Vigentes: {len(records)}  |  Con precio: {sum(1 for r in records if r['precio_usd'])}  |  Con volumen: {sum(1 for r in records if r['tope_anual_mwh'])}")

# ── chart aggregates ──────────────────────────────────────────────────────────

total      = len(df)
n_vigentes = len(vigentes)
n_ren      = int((vigentes['TIPO RECURSO'] == 'Renovable').sum())
precio_med = vigentes['precio_usd'].median()
precio_med = round(float(precio_med), 1) if pd.notna(precio_med) else None
expiring   = int(((vigentes['Fin'] >= today) & (vigentes['Fin'] <= today + pd.DateOffset(months=12))).sum())

by_year_raw = (vigentes.groupby(['fin_str', 'TIPO RECURSO'])
               .size().reset_index(name='count'))
by_year_raw['year'] = by_year_raw['fin_str'].str[:4].astype(int)
by_year = {}
for _, row in by_year_raw.iterrows():
    y = row['year']
    if y not in by_year:
        by_year[y] = {'r': 0, 'nr': 0}
    if row['TIPO RECURSO'] == 'Renovable':
        by_year[y]['r'] += row['count']
    elif row['TIPO RECURSO'] == 'No Renovable':
        by_year[y]['nr'] += row['count']
by_year_list = [{'y': y, 'r': v['r'], 'nr': v['nr']}
                for y, v in sorted(by_year.items())]

PRICE_BINS = [(0,30,'< 30'),(30,50,'30–50'),(50,60,'50–60'),
              (60,70,'60–70'),(70,80,'70–80'),(80,100,'80–100'),(100,9999,'> 100')]
price_dist = []
for lo, hi, label in PRICE_BINS:
    ren = int(((vigentes['precio_usd'] >= lo) & (vigentes['precio_usd'] < hi) &
               (vigentes['TIPO RECURSO'] == 'Renovable')).sum())
    nor = int(((vigentes['precio_usd'] >= lo) & (vigentes['precio_usd'] < hi) &
               (vigentes['TIPO RECURSO'] == 'No Renovable')).sum())
    price_dist.append({'r': label, 'ren': ren, 'nor': nor})

top10_vend = (vigentes['Vendedor'].value_counts().head(10)
              .reset_index().rename(columns={'index':'v','count':'c','Vendedor':'v'}))
# pandas version compat
if 'Vendedor' in top10_vend.columns:
    top10_vend = top10_vend.rename(columns={'Vendedor':'v'})
top10_names  = [CUIT_NAMES.get(str(r['v']), str(r['v'])) for _, r in top10_vend.iterrows()]
top10_counts = [int(r['count']) if 'count' in top10_vend.columns else int(r['c'])
                for _, r in top10_vend.iterrows()]

# ── filter options ────────────────────────────────────────────────────────────

tipos_list    = sorted(vigentes['TIPO RECURSO'].dropna().unique().tolist())
vendedores    = sorted(vigentes['Vendedor'].dropna().unique().tolist())
generadores_l = sorted(vigentes['Conj. Generador'].dropna().unique().tolist())
tc_list       = sorted(vigentes['Tipo contrato'].dropna().unique().tolist())
ini_months    = sorted(vigentes['inicio_str'].str[:7].dropna().unique().tolist())
fin_months    = sorted(vigentes['fin_str'].str[:7].dropna().unique().tolist())

def sel_opts(values):
    return ''.join(f'<option value="{v}">{v}</option>' for v in values)

def month_opts(months):
    return ''.join(f'<option value="{m}">{fmt_month(m)}</option>' for m in months)

def vend_opts_html(cuits):
    return ''.join(f'<option value="{v}">{CUIT_NAMES.get(v,v)} ({v})</option>' for v in cuits)

# ── KPI date string ───────────────────────────────────────────────────────────
MESES_ES = ['','enero','febrero','marzo','abril','mayo','junio',
            'julio','agosto','septiembre','octubre','noviembre','diciembre']
update_str = f"{today.day} de {MESES_ES[today.month]} de {today.year}"

# ── assemble HTML ─────────────────────────────────────────────────────────────

HTML = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Dashboard — Contratos de Energía CAMMESA</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap');
  *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
  :root{
    --bg:#f4f3ef;--surface:#fff;--surface2:#f9f8f5;
    --border:#e2e0d8;--border2:#ccc9be;
    --text:#1a1916;--text2:#6b6860;--text3:#9c9a93;
    --blue:#2155a3;--blue-bg:#edf2fc;--blue-text:#1a4282;
    --amber:#b06a10;--amber-bg:#fdf3e3;--amber-text:#8a5008;
    --green:#276940;--green-bg:#e8f5ee;--green-text:#1d5131;
    --red:#b52a2a;--red-bg:#fdeaea;--gray-bg:#eceae3;
    --radius:8px;--radius-lg:12px;
  }
  body{font-family:'IBM Plex Sans',system-ui,sans-serif;background:var(--bg);color:var(--text);font-size:14px;line-height:1.5;min-height:100vh}
  .header{background:var(--surface);border-bottom:1px solid var(--border);padding:16px 28px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}
  .header-left{display:flex;align-items:center;gap:12px}
  .header-icon{width:34px;height:34px;border-radius:var(--radius);background:var(--blue);display:flex;align-items:center;justify-content:center}
  .header-icon svg{width:17px;height:17px;fill:white}
  .header-title{font-size:15px;font-weight:600;letter-spacing:-.02em}
  .header-sub{font-size:11px;color:var(--text2);margin-top:1px}
  .header-date{font-size:11px;color:var(--text3);font-family:'IBM Plex Mono',monospace}
  .main{max-width:1360px;margin:0 auto;padding:24px 28px}
  .section-label{font-size:10px;font-weight:600;letter-spacing:.12em;text-transform:uppercase;color:var(--text3);margin-bottom:10px;padding-top:4px}
  .kpi-row{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin-bottom:22px}
  .kpi-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);padding:14px 13px}
  .kpi-label{font-size:10px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--text3);margin-bottom:5px}
  .kpi-value{font-size:24px;font-weight:300;letter-spacing:-.03em;font-family:'IBM Plex Mono',monospace}
  .kpi-value.c-blue{color:var(--blue)} .kpi-value.c-green{color:var(--green)} .kpi-value.c-amber{color:var(--amber)} .kpi-value.c-red{color:var(--red)} .kpi-value.c-purple{color:#6a3db5}
  .kpi-sub{font-size:11px;color:var(--text3);margin-top:3px}
  .charts-row{display:grid;grid-template-columns:1.4fr 1fr;gap:12px;margin-bottom:12px}
  .charts-row-2{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:22px}
  .chart-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);padding:18px;position:relative}
  .chart-title{font-size:11px;font-weight:600;letter-spacing:.04em;text-transform:uppercase;color:var(--text2);margin-bottom:3px}
  .chart-subtitle{font-size:11px;color:var(--text3);margin-bottom:14px}
  .chart-badge{position:absolute;top:14px;right:14px;font-size:10px;background:var(--blue-bg);color:var(--blue-text);padding:2px 7px;border-radius:20px;font-weight:600;opacity:0;transition:opacity .2s}
  .chart-badge.visible{opacity:1}
  .legend{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:12px}
  .legend-item{display:flex;align-items:center;gap:5px;font-size:11px;color:var(--text2)}
  .legend-swatch{width:10px;height:10px;border-radius:2px;flex-shrink:0}
  .donut-wrap{display:flex;align-items:center;gap:20px}
  .table-section{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-lg);overflow:hidden}
  .table-header{padding:14px 18px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px}
  .table-title{font-size:11px;font-weight:600;letter-spacing:.04em;text-transform:uppercase;color:var(--text2)}
  .count-badge{font-size:11px;font-family:'IBM Plex Mono',monospace;background:var(--gray-bg);color:var(--text2);padding:2px 8px;border-radius:20px;transition:background .2s,color .2s}
  .count-badge.filtered{background:var(--blue-bg);color:var(--blue-text)}
  .filter-grid{display:grid;grid-template-columns:repeat(4,1fr) auto;gap:8px;padding:12px 18px;border-bottom:1px solid var(--border);background:var(--surface2);align-items:end}
  .fgroup{display:flex;flex-direction:column;gap:3px}
  .fgroup label{font-size:10px;font-weight:600;letter-spacing:.06em;text-transform:uppercase;color:var(--text3)}
  .fgroup select,.fgroup input{font-family:'IBM Plex Sans',sans-serif;font-size:12px;padding:5px 8px;border:1px solid var(--border2);border-radius:6px;background:var(--surface);color:var(--text);cursor:pointer;outline:none;width:100%;transition:border-color .12s}
  .fgroup select:focus,.fgroup input:focus{border-color:var(--blue)}
  .fgroup.span2{grid-column:span 2}
  .date-pair{display:flex;gap:6px;align-items:center}
  .date-pair span{font-size:11px;color:var(--text3);flex-shrink:0}
  .btn-clear{font-family:'IBM Plex Sans',sans-serif;font-size:11px;padding:5px 12px;border:1px solid var(--border2);border-radius:6px;background:var(--surface);color:var(--text2);cursor:pointer;white-space:nowrap;transition:background .12s,color .12s;align-self:end}
  .btn-clear:hover{background:var(--red-bg);color:var(--red);border-color:var(--red)}
  .table-scroll{overflow-x:auto;max-height:640px;overflow-y:auto}
  table{width:100%;border-collapse:collapse}
  th{font-size:10px;font-weight:600;letter-spacing:.07em;text-transform:uppercase;color:var(--text3);padding:9px 11px;text-align:left;background:var(--surface2);border-bottom:1px solid var(--border);position:sticky;top:0;z-index:2;white-space:nowrap;cursor:pointer;user-select:none}
  th:hover{color:var(--text2)}
  th .si{margin-left:3px;opacity:.35;font-style:normal;font-size:10px}
  th.sa .si::after{content:'↑';opacity:1} th.sd .si::after{content:'↓';opacity:1} th:not(.sa):not(.sd) .si::after{content:'↕'}
  td{padding:7px 11px;border-bottom:1px solid var(--border);font-size:12px;white-space:nowrap;max-width:200px;overflow:hidden;text-overflow:ellipsis}
  tr:last-child td{border-bottom:none}
  tr:hover td{background:var(--surface2)}
  .mono{font-family:'IBM Plex Mono',monospace;font-size:11px}
  .tag{display:inline-block;font-size:10px;font-weight:600;padding:2px 6px;border-radius:4px;letter-spacing:.02em}
  .tag-r{background:var(--green-bg);color:var(--green-text)}
  .tag-nr{background:var(--amber-bg);color:var(--amber-text)}
  .tag-na{background:var(--gray-bg);color:var(--text3)}
  .vol-bar-wrap{display:flex;align-items:center;gap:6px;min-width:110px}
  .vol-bar{height:5px;border-radius:3px;background:var(--blue);opacity:.55;flex-shrink:0}
  .vol-txt{font-family:'IBM Plex Mono',monospace;font-size:10px;color:var(--text2);white-space:nowrap}
  .paginator{padding:10px 18px;border-top:1px solid var(--border);display:flex;align-items:center;gap:8px;font-size:12px;color:var(--text2)}
  .pag-btn{font-family:'IBM Plex Sans',sans-serif;font-size:12px;padding:4px 10px;border:1px solid var(--border2);border-radius:6px;background:var(--surface);color:var(--text);cursor:pointer;transition:background .1s}
  .pag-btn:hover:not(:disabled){background:var(--surface2)}
  .pag-btn:disabled{opacity:.35;cursor:default}
  .pag-info{font-family:'IBM Plex Mono',monospace;font-size:11px}
  .pag-total{margin-left:auto;color:var(--text3);font-size:11px}
  @media(max-width:1100px){
    .kpi-row{grid-template-columns:repeat(3,1fr)}
    .charts-row,.charts-row-2{grid-template-columns:1fr}
    .filter-grid{grid-template-columns:1fr 1fr}
    .main{padding:14px}
  }
</style>
</head>
<body>
<header class="header">
  <div class="header-left">
    <div class="header-icon">
      <svg viewBox="0 0 24 24"><path d="M13 2.05v2.02c3.95.49 7 3.85 7 7.93 0 3.21-1.81 6-4.5 7.54L13 17v5h5l-1.22-1.22C19.91 19.07 22 15.76 22 12c0-5.18-3.95-9.45-9-9.95zM11 2.05C5.95 2.55 2 6.82 2 12c0 3.76 2.09 7.07 5.22 8.78L6 22h5v-5l-2.5 2.47C6.81 18 5 15.21 5 12c0-4.08 3.05-7.44 7-7.93V2.05z"/></svg>
    </div>
    <div>
      <div class="header-title">Contratos de Energía CAMMESA</div>
      <div class="header-sub">Análisis unificado · MATE publicaciones</div>
    </div>
  </div>
  <div class="header-date">Actualizado: __UPDATE_DATE__</div>
</header>
<main class="main">
<div class="section-label">Métricas generales</div>
<div class="kpi-row">
  <div class="kpi-card"><div class="kpi-label">Total contratos</div><div class="kpi-value">__TOTAL__</div><div class="kpi-sub">todos los registros</div></div>
  <div class="kpi-card"><div class="kpi-label">Selección</div><div class="kpi-value c-blue" id="kpiVigVal">__VIGENTES__</div><div class="kpi-sub" id="kpiVigSub">contratos vigentes</div></div>
  <div class="kpi-card"><div class="kpi-label">Renovables</div><div class="kpi-value c-green" id="kpiRenVal">__RENOVABLES__</div><div class="kpi-sub" id="kpiRenSub">__PCT_REN__% del total</div></div>
  <div class="kpi-card"><div class="kpi-label">Precio mediano</div><div class="kpi-value c-amber" id="kpiPrecioVal">__PRECIO_MED__</div><div class="kpi-sub">U$S / MWh</div></div>
  <div class="kpi-card"><div class="kpi-label">Vol. anual total</div><div class="kpi-value c-purple" id="kpiVolVal">—</div><div class="kpi-sub">MWh de la selección</div></div>
  <div class="kpi-card"><div class="kpi-label">Vencen en 12 m.</div><div class="kpi-value c-red">__EXPIRING__</div><div class="kpi-sub">próximo año</div></div>
</div>
<div class="section-label">Análisis · <span id="chartNote" style="font-weight:400;color:var(--text2)">mostrando todos los contratos vigentes</span></div>
<div class="charts-row">
  <div class="chart-card">
    <div class="chart-title">Vencimientos por año</div>
    <div class="chart-subtitle">Cantidad según año de fin, por tipo de recurso</div>
    <span class="chart-badge" id="badgeAnio">filtrado</span>
    <div class="legend">
      <div class="legend-item"><span class="legend-swatch" style="background:#2155a3"></span>Renovable</div>
      <div class="legend-item"><span class="legend-swatch" style="background:#b06a10"></span>No renovable</div>
    </div>
    <div style="position:relative;width:100%;height:210px"><canvas id="chartAnio"></canvas></div>
  </div>
  <div class="chart-card">
    <div class="chart-title">Distribución de precios</div>
    <div class="chart-subtitle">Contratos con precio por rango (U$S/MWh)</div>
    <span class="chart-badge" id="badgePrecio">filtrado</span>
    <div class="legend">
      <div class="legend-item"><span class="legend-swatch" style="background:#2155a3"></span>Renovable</div>
      <div class="legend-item"><span class="legend-swatch" style="background:#b06a10"></span>No renovable</div>
    </div>
    <div style="position:relative;width:100%;height:210px"><canvas id="chartPrecios"></canvas></div>
  </div>
</div>
<div class="charts-row-2">
  <div class="chart-card">
    <div class="chart-title">Composición por tipo</div>
    <div class="chart-subtitle">Proporción según tipo de recurso</div>
    <span class="chart-badge" id="badgeDonut">filtrado</span>
    <div class="donut-wrap" style="margin-top:10px">
      <div style="position:relative;width:140px;height:140px;flex-shrink:0"><canvas id="chartDonut"></canvas></div>
      <div style="display:flex;flex-direction:column;gap:10px;font-size:13px">
        <div><div class="legend-item" style="margin-bottom:2px"><span class="legend-swatch" style="background:#2155a3"></span><strong>Renovable</strong></div><div style="font-size:20px;font-weight:300;font-family:'IBM Plex Mono',monospace;margin-left:15px"><span id="ds-r">__RENOVABLES__</span> <span style="font-size:12px;color:var(--text3)" id="ds-rp">__PCT_REN__%</span></div></div>
        <div><div class="legend-item" style="margin-bottom:2px"><span class="legend-swatch" style="background:#b06a10"></span><strong>No renovable</strong></div><div style="font-size:20px;font-weight:300;font-family:'IBM Plex Mono',monospace;margin-left:15px"><span id="ds-nr">__NO_REN__</span> <span style="font-size:12px;color:var(--text3)" id="ds-nrp">__PCT_NR__%</span></div></div>
        <div><div class="legend-item" style="margin-bottom:2px"><span class="legend-swatch" style="background:#ccc9be"></span><strong>Sin datos</strong></div><div style="font-size:20px;font-weight:300;font-family:'IBM Plex Mono',monospace;margin-left:15px"><span id="ds-na">__SIN_DATOS__</span> <span style="font-size:12px;color:var(--text3)" id="ds-nap">__PCT_NA__%</span></div></div>
      </div>
    </div>
  </div>
  <div class="chart-card">
    <div class="chart-title">Top vendedores</div>
    <div class="chart-subtitle">Por cantidad de contratos en la selección actual</div>
    <span class="chart-badge" id="badgeVend">filtrado</span>
    <div style="position:relative;width:100%;height:200px;margin-top:10px"><canvas id="chartVendedores"></canvas></div>
  </div>
</div>
<div class="section-label">Detalle de contratos vigentes</div>
<div class="table-section">
  <div class="table-header">
    <div style="display:flex;align-items:center;gap:8px">
      <div class="table-title">Contratos vigentes</div>
      <span class="count-badge" id="badgeCount">__VIGENTES__ contratos</span>
    </div>
  </div>
  <div class="filter-grid">
    <div class="fgroup span2"><label>Buscar texto</label><input type="text" id="fSearch" placeholder="Nro contrato, suministro, generador…"></div>
    <div class="fgroup"><label>Tipo recurso</label><select id="fTipo"><option value="">Todos</option><option>Renovable</option><option>No Renovable</option></select></div>
    <div class="fgroup"><label>Precio (U$S/MWh)</label><select id="fPrecio"><option value="">Todos</option><option value="0-30">Menos de 30</option><option value="30-50">30 – 50</option><option value="50-60">50 – 60</option><option value="60-70">60 – 70</option><option value="70-80">70 – 80</option><option value="80-9999">Más de 80</option><option value="null">Sin precio</option></select></div>
    <button class="btn-clear" onclick="clearFilters()" style="grid-row:span 2">Limpiar filtros</button>
    <div class="fgroup"><label>Vendedor</label><select id="fVendedor"><option value="">Todos</option>__VEND_OPTS__</select></div>
    <div class="fgroup"><label>Generador</label><select id="fGenerador"><option value="">Todos</option>__GEN_OPTS__</select></div>
    <div class="fgroup"><label>Tipo contrato</label><select id="fTipoContrato"><option value="">Todos</option>__TC_OPTS__</select></div>
    <div class="fgroup"><label>Volumen anual</label><select id="fVol"><option value="">Todos</option><option value="0-1000">Hasta 1.000 MWh</option><option value="1000-10000">1.000 – 10.000 MWh</option><option value="10000-100000">10.000 – 100.000 MWh</option><option value="100000-999999999">Más de 100.000 MWh</option><option value="null">Sin datos</option></select></div>
    <div class="fgroup span2"><label>Inicio — desde / hasta</label><div class="date-pair"><select id="fIniDesde" style="flex:1"><option value="">Cualquier inicio</option>__INI_OPTS__</select><span>→</span><select id="fIniHasta" style="flex:1"><option value="">—</option>__INI_OPTS__</select></div></div>
    <div class="fgroup span2"><label>Fin — desde / hasta</label><div class="date-pair"><select id="fFinDesde" style="flex:1"><option value="">Cualquier fin</option>__FIN_OPTS__</select><span>→</span><select id="fFinHasta" style="flex:1"><option value="">—</option>__FIN_OPTS__</select></div></div>
  </div>
  <div class="table-scroll">
    <table>
      <thead><tr>
        <th data-col="Nro contrato" style="width:78px">Contrato<i class="si"></i></th>
        <th data-col="Suministro / grupo dem." style="min-width:180px">Suministro<i class="si"></i></th>
        <th data-col="Vendedor" style="width:140px">Vendedor<i class="si"></i></th>
        <th data-col="Conj. Generador" style="width:105px">Generador<i class="si"></i></th>
        <th data-col="TIPO RECURSO" style="width:95px">Tipo<i class="si"></i></th>
        <th data-col="inicio_str" style="width:88px">Inicio<i class="si"></i></th>
        <th data-col="fin_str" style="width:88px">Fin<i class="si"></i></th>
        <th data-col="precio_usd" style="width:88px">Precio<i class="si"></i></th>
        <th data-col="tope_anual_mwh" style="width:155px">Vol. anual (MWh)<i class="si"></i></th>
        <th data-col="Tipo contrato" style="min-width:125px">Tipo contrato<i class="si"></i></th>
      </tr></thead>
      <tbody id="tableBody"></tbody>
    </table>
  </div>
  <div class="paginator">
    <button class="pag-btn" id="btnPrev" onclick="changePage(-1)">← Anterior</button>
    <span class="pag-info" id="pageInfo"></span>
    <button class="pag-btn" id="btnNext" onclick="changePage(1)">Siguiente →</button>
    <span class="pag-total" id="pagTotal"></span>
  </div>
</div>
</main>
<script>
const BLUE='#2155a3',AMBER='#b06a10',GRAY='#ccc9be';
const BA='rgba(33,85,163,.75)',AA='rgba(176,106,16,.7)';
const CUIT_NAMES=__CUIT_MAP__;
const BY_YEAR_BASE=__BY_YEAR__;
const PRICE_BASE=__PRICE_DIST__;
const PRICE_BINS=[[-Infinity,30,'< 30'],[30,50,'30-50'],[50,60,'50-60'],[60,70,'60-70'],[70,80,'70-80'],[80,100,'80-100'],[100,Infinity,'> 100']];
const ALL=__RECORDS__;

function fmtMwh(v){
  if(v==null)return '—';
  if(v>=1e6)return (v/1e6).toFixed(2).replace('.',',')+' M';
  if(v>=1e3)return (v/1e3).toFixed(1).replace('.',',')+' k';
  return v.toLocaleString('es',{maximumFractionDigits:1});
}

const CO={responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},tooltip:{bodyFont:{family:'IBM Plex Mono'}}}};
const chartAnio=new Chart(document.getElementById('chartAnio'),{type:'bar',data:{labels:BY_YEAR_BASE.map(d=>d.y),datasets:[{label:'Renovable',data:BY_YEAR_BASE.map(d=>d.r),backgroundColor:BA,stack:'s'},{label:'No renovable',data:BY_YEAR_BASE.map(d=>d.nr),backgroundColor:AA,stack:'s'}]},options:{...CO,scales:{x:{stacked:true,ticks:{font:{size:10,family:'IBM Plex Mono'},maxRotation:45,autoSkip:false}},y:{stacked:true,ticks:{font:{size:10}}}}}});
const chartPrecios=new Chart(document.getElementById('chartPrecios'),{type:'bar',data:{labels:PRICE_BASE.map(d=>d.r),datasets:[{label:'Renovable',data:PRICE_BASE.map(d=>d.ren),backgroundColor:BA,stack:'s'},{label:'No renovable',data:PRICE_BASE.map(d=>d.nor),backgroundColor:AA,stack:'s'}]},options:{...CO,scales:{x:{stacked:true,ticks:{font:{size:11}}},y:{stacked:true,ticks:{font:{size:10}}}}}});
const chartDonut=new Chart(document.getElementById('chartDonut'),{type:'doughnut',data:{labels:['Renovable','No Renovable','Sin datos'],datasets:[{data:__DONUT_DATA__,backgroundColor:[BLUE,AMBER,GRAY],borderWidth:0}]},options:{...CO,cutout:'68%'}});
const chartVend=new Chart(document.getElementById('chartVendedores'),{type:'bar',data:{labels:__TOP10_NAMES__,datasets:[{label:'Contratos',data:__TOP10_COUNTS__,backgroundColor:BA,borderRadius:3}]},options:{...CO,indexAxis:'y',scales:{x:{ticks:{font:{size:10}}},y:{ticks:{font:{size:11}}}}}});

const PER=30;
let page=0,filtered=[],sortCol=null,sortDir=1,isFiltered=false,volMax=1;

function updateCharts(data){
  const isAll=!isFiltered;
  ['badgeAnio','badgePrecio','badgeDonut','badgeVend'].forEach(id=>document.getElementById(id).classList.toggle('visible',!isAll));
  document.getElementById('chartNote').textContent=isAll?'mostrando todos los contratos vigentes':'mostrando '+data.length+' contratos filtrados';
  const ym={};
  data.forEach(r=>{const y=r.fin_str?parseInt(r.fin_str.slice(0,4)):null;if(!y)return;if(!ym[y])ym[y]={r:0,nr:0};if(r['TIPO RECURSO']==='Renovable')ym[y].r++;else if(r['TIPO RECURSO']==='No Renovable')ym[y].nr++;});
  const yrs=Object.keys(ym).map(Number).sort((a,b)=>a-b);
  chartAnio.data.labels=yrs;chartAnio.data.datasets[0].data=yrs.map(y=>ym[y].r);chartAnio.data.datasets[1].data=yrs.map(y=>ym[y].nr);chartAnio.update('none');
  const pb=PRICE_BINS.map(b=>({r:b[2],ren:0,nor:0}));
  data.forEach(r=>{if(r.precio_usd==null)return;for(let i=0;i<PRICE_BINS.length;i++){if(r.precio_usd>=PRICE_BINS[i][0]&&r.precio_usd<PRICE_BINS[i][1]){if(r['TIPO RECURSO']==='Renovable')pb[i].ren++;else if(r['TIPO RECURSO']==='No Renovable')pb[i].nor++;break;}}});
  chartPrecios.data.labels=pb.map(b=>b.r);chartPrecios.data.datasets[0].data=pb.map(b=>b.ren);chartPrecios.data.datasets[1].data=pb.map(b=>b.nor);chartPrecios.update('none');
  let dR=0,dNR=0,dNA=0;
  data.forEach(r=>{const t=r['TIPO RECURSO'];if(t==='Renovable')dR++;else if(t==='No Renovable')dNR++;else dNA++;});
  const tot=dR+dNR+dNA||1;
  chartDonut.data.datasets[0].data=[dR,dNR,dNA];chartDonut.update('none');
  document.getElementById('ds-r').textContent=dR.toLocaleString('es');
  document.getElementById('ds-rp').textContent=Math.round(dR/tot*100)+'%';
  document.getElementById('ds-nr').textContent=dNR.toLocaleString('es');
  document.getElementById('ds-nrp').textContent=Math.round(dNR/tot*100)+'%';
  document.getElementById('ds-na').textContent=dNA.toLocaleString('es');
  document.getElementById('ds-nap').textContent=Math.round(dNA/tot*100)+'%';
  const vm={};
  data.forEach(r=>{const v=r['Vendedor']||'—';vm[v]=(vm[v]||0)+1;});
  const topV=Object.entries(vm).sort((a,b)=>b[1]-a[1]).slice(0,10);
  chartVend.data.labels=topV.map(([c])=>CUIT_NAMES[c]||c);
  chartVend.data.datasets[0].data=topV.map(([,n])=>n);
  chartVend.update('none');
  const prices=data.map(r=>r.precio_usd).filter(v=>v!=null).sort((a,b)=>a-b);
  const med=prices.length?prices[Math.floor(prices.length/2)]:null;
  const totalVol=data.reduce((s,r)=>s+(r.tope_anual_mwh||0),0);
  document.getElementById('kpiVigVal').textContent=data.length.toLocaleString('es');
  document.getElementById('kpiVigSub').textContent=isAll?'contratos vigentes':'contratos filtrados';
  document.getElementById('kpiRenVal').textContent=dR.toLocaleString('es');
  document.getElementById('kpiRenSub').textContent=Math.round(dR/tot*100)+'% de la selección';
  document.getElementById('kpiPrecioVal').textContent=med!=null?med.toFixed(1).replace('.',','):'—';
  document.getElementById('kpiVolVal').textContent=fmtMwh(totalVol||null);
  volMax=Math.max(...data.map(r=>r.tope_anual_mwh||0),1);
}

function applyFilters(){
  const search=document.getElementById('fSearch').value.toLowerCase().trim();
  const tipo=document.getElementById('fTipo').value;
  const vend=document.getElementById('fVendedor').value;
  const gen=document.getElementById('fGenerador').value;
  const tc=document.getElementById('fTipoContrato').value;
  const precio=document.getElementById('fPrecio').value;
  const vol=document.getElementById('fVol').value;
  const iniD=document.getElementById('fIniDesde').value;
  const iniH=document.getElementById('fIniHasta').value;
  const finD=document.getElementById('fFinDesde').value;
  const finH=document.getElementById('fFinHasta').value;
  isFiltered=!!(search||tipo||vend||gen||tc||precio||vol||iniD||iniH||finD||finH);
  filtered=ALL.filter(r=>{
    if(tipo&&r['TIPO RECURSO']!==tipo)return false;
    if(vend&&r['Vendedor']!==vend)return false;
    if(gen&&r['Conj. Generador']!==gen)return false;
    if(tc&&r['Tipo contrato']!==tc)return false;
    if(precio){if(precio==='null'){if(r.precio_usd!=null)return false;}else{const[lo,hi]=precio.split('-').map(Number);if(r.precio_usd==null||r.precio_usd<lo||r.precio_usd>=hi)return false;}}
    if(vol){if(vol==='null'){if(r.tope_anual_mwh!=null)return false;}else{const[lo,hi]=vol.split('-').map(Number);if(r.tope_anual_mwh==null||r.tope_anual_mwh<lo||r.tope_anual_mwh>=hi)return false;}}
    if(iniD&&r.inicio_str.slice(0,7)<iniD)return false;
    if(iniH&&r.inicio_str.slice(0,7)>iniH)return false;
    if(finD&&r.fin_str.slice(0,7)<finD)return false;
    if(finH&&r.fin_str.slice(0,7)>finH)return false;
    if(search){const hay=(r['Nro contrato']+' '+r['Suministro / grupo dem.']+' '+r['Conj. Generador']+' '+(r['Vendedor']||'')+' '+(CUIT_NAMES[r['Vendedor']]||'')).toLowerCase();if(!hay.includes(search))return false;}
    return true;
  });
  if(sortCol){filtered.sort((a,b)=>{let av=a[sortCol]??'',bv=b[sortCol]??'';return typeof av==='number'&&typeof bv==='number'?sortDir*(av-bv):sortDir*String(av).localeCompare(String(bv),'es');});}
  page=0;
  document.getElementById('badgeCount').classList.toggle('filtered',isFiltered);
  updateCharts(filtered);
  render();
}

function clearFilters(){
  ['fSearch','fTipo','fVendedor','fGenerador','fTipoContrato','fPrecio','fVol','fIniDesde','fIniHasta','fFinDesde','fFinHasta'].forEach(id=>{document.getElementById(id).value='';});
  applyFilters();
}

document.querySelectorAll('th[data-col]').forEach(th=>{
  th.addEventListener('click',()=>{
    const col=th.dataset.col;
    sortDir=(sortCol===col)?-sortDir:1;sortCol=col;
    document.querySelectorAll('th').forEach(t=>t.classList.remove('sa','sd'));
    th.classList.add(sortDir===1?'sa':'sd');
    applyFilters();
  });
});

function render(){
  const start=page*PER,end=Math.min(start+PER,filtered.length);
  document.getElementById('tableBody').innerHTML=filtered.slice(start,end).map(r=>{
    const tipo=r['TIPO RECURSO'];
    const tc=tipo==='Renovable'?'tag-r':tipo==='No Renovable'?'tag-nr':'tag-na';
    const pr=r.precio_usd!=null?r.precio_usd.toFixed(1)+' U$S':'—';
    const su=r['Suministro / grupo dem.']||'';
    const vn=CUIT_NAMES[r['Vendedor']]||r['Vendedor']||'—';
    const v=r.tope_anual_mwh;
    const barW=v!=null?Math.round(Math.min(v/volMax,1)*60):0;
    const volCell=v!=null
      ?'<div class="vol-bar-wrap"><div class="vol-bar" style="width:'+barW+'px"></div><span class="vol-txt">'+fmtMwh(v)+'</span></div>'
      :'<span class="vol-txt" style="color:var(--text3)">—</span>';
    return '<tr>'
      +'<td class="mono">'+r['Nro contrato']+'</td>'
      +'<td title="'+su.replace(/"/g,'&quot;')+'">'+(su.length>34?su.slice(0,34)+'…':su)+'</td>'
      +'<td title="'+(r['Vendedor']||'')+'">'+vn+'</td>'
      +'<td class="mono" style="font-size:11px">'+(r['Conj. Generador']||'—')+'</td>'
      +'<td><span class="tag '+tc+'">'+(tipo||'—')+'</span></td>'
      +'<td class="mono">'+r.inicio_str+'</td>'
      +'<td class="mono">'+r.fin_str+'</td>'
      +'<td class="mono">'+pr+'</td>'
      +'<td>'+volCell+'</td>'
      +'<td style="font-size:11px;color:var(--text2)">'+r['Tipo contrato']+'</td>'
      +'</tr>';
  }).join('');
  document.getElementById('pageInfo').textContent=filtered.length?(start+1)+'–'+end:'0';
  document.getElementById('pagTotal').textContent=filtered.length+' contratos';
  document.getElementById('badgeCount').textContent=filtered.length+' contratos';
  document.getElementById('btnPrev').disabled=page===0;
  document.getElementById('btnNext').disabled=end>=filtered.length;
}

window.changePage=function(d){page+=d;render();};
['fTipo','fVendedor','fGenerador','fTipoContrato','fPrecio','fVol','fIniDesde','fIniHasta','fFinDesde','fFinHasta'].forEach(id=>
  document.getElementById(id).addEventListener('change',applyFilters));
let st;
document.getElementById('fSearch').addEventListener('input',function(){clearTimeout(st);st=setTimeout(applyFilters,250);});
applyFilters();
</script>
</body>
</html>
"""

# ── compute donut data ────────────────────────────────────────────────────────
n_no_ren  = int((vigentes['TIPO RECURSO'] == 'No Renovable').sum())
n_sin     = int(n_vigentes - n_ren - n_no_ren)
tot       = n_vigentes or 1
pct_ren   = round(n_ren / tot * 100)
pct_nr    = round(n_no_ren / tot * 100)
pct_na    = 100 - pct_ren - pct_nr

# ── inject all values ─────────────────────────────────────────────────────────
HTML = (HTML
    .replace('__UPDATE_DATE__',  update_str)
    .replace('__TOTAL__',        f'{total:,}'.replace(',','.'))
    .replace('__VIGENTES__',     f'{n_vigentes:,}'.replace(',','.'))
    .replace('__RENOVABLES__',   f'{n_ren:,}'.replace(',','.'))
    .replace('__NO_REN__',       f'{n_no_ren:,}'.replace(',','.'))
    .replace('__SIN_DATOS__',    f'{n_sin:,}'.replace(',','.'))
    .replace('__PCT_REN__',      str(pct_ren))
    .replace('__PCT_NR__',       str(pct_nr))
    .replace('__PCT_NA__',       str(pct_na))
    .replace('__PRECIO_MED__',   str(precio_med).replace('.', ',') if precio_med else '—')
    .replace('__EXPIRING__',     f'{expiring:,}'.replace(',','.'))
    .replace('__VEND_OPTS__',    vend_opts_html(vendedores))
    .replace('__GEN_OPTS__',     sel_opts(generadores_l))
    .replace('__TC_OPTS__',      sel_opts(tc_list))
    .replace('__INI_OPTS__',     month_opts(ini_months))
    .replace('__FIN_OPTS__',     month_opts(fin_months))
    .replace('__CUIT_MAP__',     json.dumps(CUIT_NAMES, ensure_ascii=False))
    .replace('__BY_YEAR__',      json.dumps(by_year_list, ensure_ascii=False))
    .replace('__PRICE_DIST__',   json.dumps(price_dist, ensure_ascii=False))
    .replace('__DONUT_DATA__',   json.dumps([n_ren, n_no_ren, n_sin]))
    .replace('__TOP10_NAMES__',  json.dumps(top10_names, ensure_ascii=False))
    .replace('__TOP10_COUNTS__', json.dumps(top10_counts))
    .replace('__RECORDS__',      json.dumps(records, ensure_ascii=False, separators=(',',':')))
)

OUTPUT.write_text(HTML, encoding='utf-8')
print(f"Dashboard generado: {OUTPUT}  ({round(len(HTML.encode())/1024)} KB)")
