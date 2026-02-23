# Checklist de tareas pendientes

Actualizado: 2026-02-23

## Estado general
- Objetivo principal: completar descargas faltantes ESGF.
- Bloqueos actuales:
  1. Gateway en `pairing required` (impide crear cron y pruebas end-to-end desde OpenClaw).
  2. Nodo ESGF `esgf-data02.diasjp.net` caído (impide descargar MIROC6 r7i1p1f1 s2020).

---

## Tareas del usuario

- [ ] **U1. Hacer pairing del gateway**
  - Acción: abrir `openclaw dashboard` y aprobar emparejamiento.
  - Resultado esperado: desaparece error `pairing required`.
  - **Dependencias:** ninguna.

- [ ] **U2. Confirmar cuando U1 esté hecha**
  - Acción: avisar con “listo”.
  - **Dependencias:** U1.

---

## Tareas del asistente

- [x] **A1. Corregir configuración de Kimi API key**
  - Estado: hecho (`moonshot:default` actualizado).
  - **Dependencias:** ninguna.

- [ ] **A2. Verificar Kimi con prueba real**
  - Acción: lanzar test de inferencia usando alias `Kimi`.
  - **Dependencias:** U1 (gateway sin pairing).

- [x] **A3. Implementar fallback `gn -> gr` en scripts**
  - Estado: hecho en `descargar_datos.py` y `descargar_faltantes.py`.
  - **Dependencias:** ninguna.

- [x] **A4. Implementar script de reintento MIROC6 s2020**
  - Archivo: `reintentar_miroc6_r7_2020.py`.
  - **Dependencias:** ninguna.

- [ ] **A5. Programar reintento automático vía cron OpenClaw**
  - Acción: crear job horario estable en cron.
  - **Dependencias:** U1.

- [ ] **A6. Monitorear resultado final de descarga MIROC6 s2020**
  - Acción: confirmar archivo descargado en `datos/MIROC6/r7i1p1f1/s2020/`.
  - **Dependencias:** A5 + disponibilidad del nodo `esgf-data02.diasjp.net`.

---

## Dependencias (resumen rápido)

1. **U1** → habilita **A2** y **A5**.
2. **A5** + nodo ESGF disponible → habilitan **A6**.
3. **A6** completada → objetivo principal finalizado.

---

## Notas técnicas

- Casos EC-Earth3 faltantes reportados no aparecen publicados en ESGF (no descargables por ahora).
- Caso pendiente realmente recuperable: `MIROC6 / r7i1p1f1 / s2020`, sujeto a disponibilidad del nodo japonés.
