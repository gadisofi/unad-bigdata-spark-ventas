# Tarea 3 - Big Data con Apache Spark

## Autor
Jeyson Antonio Villamil Genez

Universidad Nacional Abierta y a Distancia - UNAD

## Objetivo
Analizar transacciones comerciales mediante Apache Spark
para identificar patrones de ventas y demanda que apoyen
la toma de decisiones y la planificación de inventarios.

## Dataset
Online Retail - UCI Machine Learning Repository

Fuente:
https://archive.ics.uci.edu/dataset/352/online+retail

Periodo: diciembre de 2010 a diciembre de 2011.

Registros originales: 541.909.

## Herramientas
- Ubuntu Server 22.04.5 LTS
- Python 3.10
- Apache Spark 3.5.8
- PySpark
- openpyxl
- Matplotlib

## Metodologia
1. Conversion del archivo Excel a CSV.
2. Exploracion inicial con Spark.
3. Diagnostico de valores nulos y cancelaciones.
4. Clasificacion y limpieza de las transacciones.
5. Calculo de indicadores comerciales.
6. Analisis de ventas mensuales.
7. Generacion de graficas.

## Resultados

| Indicador | Resultado |
|---|---:|
| Registros originales | 541.909 |
| Ventas positivas | 530.104 |
| Registros para revision | 11.805 |
| Unidades vendidas | 5.588.376 |
| Facturas diferentes | 19.960 |
| Ingresos brutos positivos (GBP) | 10.666.684,54 |

Los ingresos corresponden a transacciones positivas
seleccionadas, sin descontar devoluciones.

## Organizacion

- codigo/: programas Python del procesamiento batch.
- graficas/: visualizaciones del analisis mensual.

## Reproduccion

Descargar el dataset original desde UCI y extraer
Online Retail.xlsx en la carpeta datos/ del proyecto.

Los programas fueron desarrollados en un entorno
Ubuntu Server con Spark instalado.

Los scripts originales utilizan rutas relativas y,
en algunos casos, rutas absolutas de la maquina virtual.
Antes de ejecutarlos en otro equipo deben ajustarse
esas rutas y las dependencias correspondientes.

## Alcance

Este repositorio documenta el procesamiento batch.
El procesamiento en tiempo real con Kafka y Spark
Streaming se encuentra pendiente de incorporacion.

## Nota

El dataset no contiene existencias de inventario,
costos de adquisicion ni tiempos de reposicion.
El analisis permite estudiar demanda historica,
pero no calcular directamente el inventario disponible.
