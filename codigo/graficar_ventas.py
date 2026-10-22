from pathlib import Path
BASE = Path(__file__).resolve().parent.parent
import csv
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

base = BASE
origen = base / "datos" / "resultados_mensuales"
destino = base / "graficas"
destino.mkdir(exist_ok=True)

archivos = list(origen.glob("part-*.csv"))
if len(archivos) != 1:
    raise RuntimeError("Se esperaba un archivo CSV de Spark.")

with archivos[0].open(encoding="utf-8-sig", newline="") as f:
    datos = sorted(
        csv.DictReader(f),
        key=lambda fila: fila["Mes"]
    )

meses = [fila["Mes"] for fila in datos]
ingresos = [float(fila["Ingresos"]) for fila in datos]
unidades = [int(fila["Unidades"]) for fila in datos]

for valores, titulo, etiqueta, archivo in [
    (ingresos, "Ingresos mensuales - Online Retail",
     "Ingresos (GBP)", "ingresos_mensuales.png"),
    (unidades, "Unidades vendidas por mes - Online Retail",
     "Unidades", "unidades_mensuales.png")
]:
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(meses, valores, marker="o")
    ax.set_title(titulo)
    ax.set_ylabel(etiqueta)
    ax.set_xlabel("Mes")
    ax.tick_params(axis="x", rotation=45)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(destino / archivo, dpi=160)
    plt.close(fig)
    print("Grafica generada:", archivo)

print("Graficas guardadas en:", destino)
