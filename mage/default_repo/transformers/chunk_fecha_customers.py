from datetime import datetime, timezone, timedelta

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def _add_months(dt, months):
    # Suma meses sin dependencias externas
    y = dt.year + (dt.month - 1 + months) // 12
    m = (dt.month - 1 + months) % 12 + 1
    d = min(dt.day, [31,
                     29 if y % 4 == 0 and (y % 100 != 0 or y % 400 == 0) else 28,
                     31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1])
    return dt.replace(year=y, month=m, day=d)


def _add_years(dt, years):
    try:
        return dt.replace(year=dt.year + years)
    except ValueError:
        # 29 feb → 28 feb si el nuevo año no es bisiesto
        return dt.replace(month=2, day=28, year=dt.year + years)


@transformer
def chunk_fecha(*args, **kwargs):
    """
    Genera tramos entre [fecha_inicio, fecha_fin) con tamaño configurable.
    Runtime vars:
      - fecha_inicio (ISO UTC, ej: '2000-01-01T00:00:00Z')
      - fecha_fin    (ISO UTC, ej: '2050-01-01T00:00:00Z')
      - chunk        ('day' | 'week' | 'month' | 'quarter' | 'year') [default: 'week']
      - page_size    (int) [default: 200]

    parámetros UTC y segmentación día/semana
    devuelve campos para registrar métricas por tramo (páginas, inserts/updates, duración).
    """
    fi = kwargs.get('fecha_inicio')
    ff = kwargs.get('fecha_fin')
    chunk = (kwargs.get('chunk') or 'week').lower()   # default week
    page_size = int(kwargs.get('page_size') or 200)

    if not fi or not ff:
        raise Exception("Variables faltantes: fecha_inicio y/o fecha_fin")

    start = datetime.fromisoformat(fi.replace('Z', '+00:00')).astimezone(timezone.utc)
    end   = datetime.fromisoformat(ff.replace('Z', '+00:00')).astimezone(timezone.utc)
    if end <= start:
        raise Exception("fecha_fin debe ser mayor que fecha_inicio")

    tramos = []
    cursor = start
    tramo_id = 1

    while cursor < end:
        if chunk in ('day', 'daily'):
            tramo_end = min(cursor + timedelta(days=1), end)
        elif chunk in ('week', 'weekly', 'semana', 'semanal'):
            tramo_end = min(cursor + timedelta(days=7), end)
        elif chunk in ('quarter', 'q', 'trim'):
            tramo_end = min(_add_months(cursor, 3), end)
        elif chunk in ('year', 'y', 'anual', 'año', 'ano'):
            tramo_end = min(_add_years(cursor, 1), end)
        else:  # month
            tramo_end = min(_add_months(cursor, 1), end)

        tramos.append({
            'tramo_id': tramo_id,
            'start': cursor.isoformat().replace('+00:00', 'Z'),
            'end': tramo_end.isoformat().replace('+00:00', 'Z'),
            'page_size': page_size,
            # Estructura para métricas por tramo
            'metrics': {
                'pages_read': 0,
                'rows_read': 0,
                'rows_inserted': 0,
                'rows_updated': 0,
                'duration_secs': 0.0,
                'status': 'pending'
            }
        })
        cursor = tramo_end
        tramo_id += 1

    print(f"[chunk_fecha] chunk={chunk} | page_size={page_size} | tramos={len(tramos)}")
    return tramos
