# app.py - Dashboard AVO Carbon Group
"""Dashboard Flask pour visualiser les prix des métaux et les taux de change ECB, avec export Excel.
"""
from flask import Flask, render_template, jsonify, request, send_file
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta, date
import logging
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from io import BytesIO
import calendar

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ===============================
# CONFIGURATION
# ===============================
app = Flask(__name__)

# Configuration base de données
DB_CONFIG = {
    "user": "administrationSTS",
    "password": "St$@0987",
    "host": "avo-adb-002.postgres.database.azure.com",
    "port": "5432",
    "database": "LME_DB",
    "sslmode": "require"
}
# ===============================
# FONCTIONS DATABASE GÉNÉRALES
# ===============================
def get_db_connection():
    """Créer une connexion à PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Erreur de connexion à la base de données: {e}")
        return None

# ===============================
# PARTIE MÉTAUX
# ===============================
def get_latest_prices():
    """Récupère les prix les plus récents pour chaque type de métal."""
    conn = get_db_connection()
    if not conn:
        return []

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                WITH latest_prices AS (
                    SELECT 
                        id, 
                        metal_type,
                        ROW_NUMBER() OVER (PARTITION BY metal_type ORDER BY price_date DESC, created_at DESC) as rn
                    FROM 
                        metal_prices
                )
                SELECT 
                    p.*
                FROM 
                    metal_prices p
                JOIN 
                    latest_prices lp ON p.id = lp.id
                WHERE 
                    lp.rn = 1
                ORDER BY 
                    p.metal_type;
            """
            cur.execute(query)
            prices = cur.fetchall()
            return prices
    except Exception as e:
        logger.error(f"Erreur get_latest_prices: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_price_history(days=None, metal_type=None, start_date=None, end_date=None):
    """
    Récupère l'historique des prix avec filtres.
    """
    conn = get_db_connection()
    if not conn:
        return []

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT 
                    metal_type, 
                    price, 
                    currency, 
                    unit, 
                    source_url,
                    price_date,
                    created_at 
                FROM 
                    metal_prices
                WHERE 1=1
            """
            params = []

            # Date de début
            if start_date:
                query += " AND price_date >= %s"
                params.append(start_date)
            
            # Date de fin (incluse)
            if end_date:
                end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
                query += " AND price_date <= %s"
                params.append(end_date_obj)
            # Si pas de dates mais param days
            elif days:
                start_date_obj = datetime.now() - timedelta(days=int(days))
                query += " AND price_date >= %s"
                params.append(start_date_obj.date())

            # Type de métal
            if metal_type and metal_type.lower() != 'all':
                query += " AND metal_type = %s"
                params.append(metal_type)

            query += " ORDER BY metal_type, price_date DESC, created_at DESC"
            
            cur.execute(query, params)
            history = cur.fetchall()
            return history
    except Exception as e:
        logger.error(f"Erreur get_price_history: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_statistics():
    """
    Calcule les statistiques (nombre total d'enregistrements, nombre de métaux, 
    et variation 24h pour chaque métal).
    """
    conn = get_db_connection()
    if not conn:
        return {'total_records': 0, 'total_metals': 0, 'variations': []}

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. Nombre total d'enregistrements et nombre de métaux
            cur.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT metal_type) as total_metals
                FROM 
                    metal_prices;
            """)
            summary = cur.fetchone()
            
            total_records = summary['total_records']
            total_metals = summary['total_metals']

            # 2. Variation 24h
            query_variation = """
                WITH RankedPrices AS (
                    SELECT 
                        id,
                        metal_type, 
                        price, 
                        currency,
                        price_date,
                        created_at,
                        ROW_NUMBER() OVER (PARTITION BY metal_type ORDER BY price_date DESC, created_at DESC) as rn 
                    FROM 
                        metal_prices
                ),
                LatestPrices AS (
                    SELECT 
                        metal_type, 
                        price as current_price,
                        currency,
                        price_date as current_date
                    FROM 
                        RankedPrices
                    WHERE 
                        rn = 1
                ),
                PreviousPrices AS (
                    SELECT 
                        metal_type, 
                        price as previous_price
                    FROM 
                        RankedPrices
                    WHERE
                        rn = 2
                )
                SELECT 
                    l.metal_type, 
                    l.current_price,
                    l.currency,
                    p.previous_price,
                    CASE 
                        WHEN p.previous_price IS NOT NULL AND p.previous_price != 0 
                        THEN ((l.current_price - p.previous_price) / p.previous_price) * 100 
                        ELSE NULL 
                    END AS variation_percent
                FROM 
                    LatestPrices l
                LEFT JOIN 
                    PreviousPrices p ON l.metal_type = p.metal_type
                ORDER BY 
                    l.metal_type;
            """
            cur.execute(query_variation)
            variations = cur.fetchall()

            return {
                'total_records': total_records,
                'total_metals': total_metals,
                'variations': variations
            }
    except Exception as e:
        logger.error(f"Erreur get_statistics: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {'total_records': 0, 'total_metals': 0, 'variations': []}
    finally:
        if conn:
            conn.close()

def get_sync_logs(limit=10):
    """Récupère les logs de synchronisation (structure adaptée à ta table actuelle)."""
    conn = get_db_connection()
    if not conn:
        return []

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # adapter si ta table a d'autres colonnes
            query = """
                SELECT 
                    id, 
                    sync_type,
                    status,
                    metals_updated,
                    error_message,
                    duration_seconds,
                    created_at
                FROM 
                    sync_logs
                ORDER BY 
                    created_at DESC
                LIMIT %s;
            """
            cur.execute(query, (limit,))
            logs = cur.fetchall()
            return logs
    except Exception as e:
        logger.error(f"Erreur get_sync_logs: {e}")
        return []
    finally:
        if conn:
            conn.close()

# ===============================
# PARTIE  TAUX DE CHANGE
# ===============================
def month_to_range(month_str: str):
    """
    month_str: 'YYYY-MM'
    returns (start_date, end_date) as date objects
    """
    if not month_str:
        return None, None
    try:
        y, m = map(int, month_str.split('-'))
        last_day = calendar.monthrange(y, m)[1]
        return date(y, m, 1), date(y, m, last_day)
    except Exception:
        return None, None
    
def get_ecb_rates(start_date=None, end_date=None, quote_currency=None, month=None):
    """
    Récupère les taux de change ECB depuis la table ecb_exchange_rates.
    Filtres:
      - start_date, end_date : 'YYYY-MM-DD' (optionnels)
      - quote_currency : code devise (ex: 'USD') ou None pour toutes.
      - month : 'YYYY-MM' pour filtrer par mois
    """
    conn = get_db_connection()
    if not conn:
        return []

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT 
                    ref_date,
                    base_currency,
                    quote_currency,
                    rate,
                    source_url,
                    metadata
                FROM 
                    ecb_exchange_rates
                WHERE 1=1
            """
            params = []

            # Handle month filter first (overrides start_date/end_date)
            if month:
                sd, ed = month_to_range(month)
                if sd and ed:
                    query += " AND ref_date >= %s AND ref_date <= %s"
                    params.extend([sd, ed])
            else:
                # Handle individual date filters
                if start_date:
                    try:
                        sd = datetime.strptime(start_date, "%Y-%m-%d").date()
                    except ValueError:
                        sd = None
                    if sd:
                        query += " AND ref_date >= %s"
                        params.append(sd)

                if end_date:
                    try:
                        ed = datetime.strptime(end_date, "%Y-%m-%d").date()
                    except ValueError:
                        ed = None
                    if ed:
                        query += " AND ref_date <= %s"
                        params.append(ed)

                # Si aucune date fournie, on limite par défaut aux 365 derniers jours
                if not start_date and not end_date:
                    query += " AND ref_date >= %s"
                    params.append(datetime.now().date() - timedelta(days=365))

            if quote_currency and quote_currency.lower() != 'all':
                query += " AND quote_currency = %s"
                params.append(quote_currency.upper())

            query += " ORDER BY ref_date DESC, quote_currency ASC"

            cur.execute(query, params)
            rows = cur.fetchall()
            return rows
    except Exception as e:
        logger.error(f"Erreur get_ecb_rates: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_florent_report_data(year, month):
    """
    Récupère les données pour le rapport Florent:
    - Closing rate du mois sélectionné
    - Closing rate du mois précédent (M-1)
    - Moyenne YTD (du 1er janvier jusqu'à la fin du mois sélectionné)
    """
    conn = get_db_connection()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
            WITH MonthlyData AS (
                SELECT 
                    quote_currency,
                    rate,
                    ref_date,
                    LAST_VALUE(rate) OVER (
                        PARTITION BY quote_currency, DATE_TRUNC('month', ref_date) 
                        ORDER BY ref_date 
                        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) as month_closing
                FROM ecb_exchange_rates
                WHERE EXTRACT(YEAR FROM ref_date) = %s
            )
            SELECT 
                quote_currency,
                MAX(CASE WHEN EXTRACT(MONTH FROM ref_date) = %s THEN month_closing END) as closing_rate,
                MAX(CASE WHEN EXTRACT(MONTH FROM ref_date) = %s - 1 THEN month_closing END) as period_rate,
                AVG(rate) FILTER (WHERE EXTRACT(MONTH FROM ref_date) <= %s) as ytd_average
            FROM MonthlyData
            GROUP BY quote_currency
            ORDER BY quote_currency;
            """
            cur.execute(query, (year, month, month, month))
            return cur.fetchall()
    except Exception as e:
        logger.error(f"Erreur get_florent_report_data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def get_monthly_fx_summary(year=None, month=None, quote_currency=None):
    """
    Récupère le résumé mensuel dynamique des taux FX:
    - Closing Rate: dernier taux disponible du mois sélectionné
    - Period Rate (M-1): dernier taux du mois précédent
    - Average YTD: moyenne depuis le 1er janvier jusqu'à la fin du mois sélectionné
    
    Si year/month non fournis, utilise le mois actuel
    """
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        # Si pas de date fournie, utiliser le mois actuel
        if not year or not month:
            today = datetime.now()
            year = today.year
            month = today.month
        else:
            year = int(year)
            month = int(month)
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
            WITH MonthlyClosing AS (
                -- Closing Rate: dernier taux disponible du mois sélectionné
                SELECT DISTINCT ON (quote_currency)
                    quote_currency,
                    rate as closing_rate,
                    ref_date as closing_date
                FROM ecb_exchange_rates
                WHERE EXTRACT(YEAR FROM ref_date) = %s
                  AND EXTRACT(MONTH FROM ref_date) = %s
                ORDER BY quote_currency, ref_date DESC
            ),
            PreviousMonthClosing AS (
                -- Period Rate: dernier taux du mois précédent (M-1)
                SELECT DISTINCT ON (quote_currency)
                    quote_currency,
                    rate as period_rate,
                    ref_date as period_date
                FROM ecb_exchange_rates
                WHERE (
                    (EXTRACT(YEAR FROM ref_date) = %s AND EXTRACT(MONTH FROM ref_date) = %s - 1)
                    OR
                    (EXTRACT(YEAR FROM ref_date) = %s - 1 AND EXTRACT(MONTH FROM ref_date) = 12 AND %s = 1)
                )
                ORDER BY quote_currency, ref_date DESC
            ),
            YTDAverage AS (
                -- Average YTD: moyenne depuis le 1er janvier jusqu'à la fin du mois sélectionné
                SELECT 
                    quote_currency,
                    AVG(rate) as ytd_average
                FROM ecb_exchange_rates
                WHERE EXTRACT(YEAR FROM ref_date) = %s
                  AND EXTRACT(MONTH FROM ref_date) <= %s
                GROUP BY quote_currency
            )
            SELECT 
                mc.quote_currency,
                mc.closing_rate,
                mc.closing_date,
                pmc.period_rate,
                pmc.period_date,
                ytd.ytd_average
            FROM MonthlyClosing mc
            LEFT JOIN PreviousMonthClosing pmc ON mc.quote_currency = pmc.quote_currency
            LEFT JOIN YTDAverage ytd ON mc.quote_currency = ytd.quote_currency
            WHERE 1=1
            """
            
            params = [
                year, month,  # MonthlyClosing
                year, month, year, month,  # PreviousMonthClosing
                year, month  # YTDAverage
            ]
            
            # Filtre optionnel par devise
            if quote_currency and quote_currency.lower() != 'all':
                query += " AND mc.quote_currency = %s"
                params.append(quote_currency.upper())
            
            query += " ORDER BY mc.quote_currency"
            
            cur.execute(query, params)
            return cur.fetchall()
            
    except Exception as e:
        logger.error(f"Erreur get_monthly_fx_summary: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []
    finally:
        conn.close()


# Add this NEW route for the monthly summary API

@app.route('/ecb/monthly-summary')
def api_monthly_fx_summary():
    """
    API pour récupérer le résumé mensuel dynamique des taux FX.
    Paramètres:
      - year (optionnel): année (défaut: année actuelle)
      - month (optionnel): mois 1-12 (défaut: mois actuel)
      - quote_currency (optionnel): devise à filtrer
    """
    year = request.args.get('year', type=int)
    month = request.args.get('month', type=int)
    quote_currency = request.args.get('quote_currency')
    
    summary = get_monthly_fx_summary(year, month, quote_currency)
    
    def serialize_summary(row):
        d = dict(row)
        # Serialize dates
        if isinstance(d.get('closing_date'), (date, datetime)):
            d['closing_date'] = d['closing_date'].isoformat()
        if isinstance(d.get('period_date'), (date, datetime)):
            d['period_date'] = d['period_date'].isoformat()
        # Serialize decimal values
        if d.get('closing_rate') is not None:
            d['closing_rate'] = float(d['closing_rate'])
        if d.get('period_rate') is not None:
            d['period_rate'] = float(d['period_rate'])
        if d.get('ytd_average') is not None:
            d['ytd_average'] = float(d['ytd_average'])
        return d
    
    data = [serialize_summary(r) for r in summary]
    
    # Add metadata about the period
    metadata = {
        'year': year or datetime.now().year,
        'month': month or datetime.now().month,
        'month_name': datetime(year or datetime.now().year, month or datetime.now().month, 1).strftime('%B %Y'),
        'is_current_month': (year or datetime.now().year) == datetime.now().year and (month or datetime.now().month) == datetime.now().month
    }
    
    return jsonify({
        'status': 'success',
        'data': data,
        'metadata': metadata
    })

# ===============================
# ROUTES FLASK
# ===============================
@app.route('/')
def index():
    """Route principale pour servir le front-end HTML."""
    return render_template('index.html')

@app.route('/health')
def health_check():
    """Point de contrôle de santé."""
    try:
        conn = get_db_connection()
        if conn:
            conn.close()
            return jsonify({'status': 'ok', 'db': 'connected'}), 200
        else:
            return jsonify({'status': 'error', 'db': 'disconnected'}), 500
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ---------- API MÉTAUX ----------

@app.route('/api/prices/latest')
def api_latest_prices():
    """API pour les derniers prix."""
    prices = get_latest_prices()
    return jsonify({'status': 'success', 'data': prices})

@app.route('/api/prices/history')
def api_price_history():
    days = request.args.get('days', type=int)
    metal_type = request.args.get('metal_type')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    month = request.args.get('month')  # NEW

    if metal_type and metal_type.lower() == 'all':
        metal_type = None

    # If month is provided, override date range
    if month:
        sd, ed = month_to_range(month)
        if sd and ed:
            start_date = sd.isoformat()
            end_date = ed.isoformat()
            days = None  # ignore days

    history = get_price_history(days, metal_type, start_date, end_date)

    def serialize_history(item):
        if isinstance(item.get('price_date'), (date, datetime)):
            item['price_date'] = item['price_date'].isoformat()
        if isinstance(item.get('created_at'), (date, datetime)):
            item['created_at'] = item['created_at'].isoformat()
        return item

    serialized_history = [serialize_history(dict(item)) for item in history]
    return jsonify({'status': 'success', 'data': serialized_history})

@app.route('/api/statistics')
def api_statistics():
    """API pour les statistiques globales et les variations."""
    stats = get_statistics()
    return jsonify({'status': 'success', 'data': stats})

@app.route('/api/sync/logs')
def api_sync_logs():
    """API pour les logs de synchronisation."""
    logs = get_sync_logs()
    serialized_logs = []
    for log in logs:
        log_dict = dict(log)
        if log_dict.get('created_at'):
            log_dict['created_at'] = log_dict['created_at'].isoformat()
        serialized_logs.append(log_dict)
        
    return jsonify({'status': 'success', 'data': serialized_logs})

@app.route('/export/excel')
def export_excel():
    """
    Route pour exporter l'historique des prix au format Excel.
    Les données sont pivotées: Date en colonne, Métal en ligne.
    """
    try:
        days = request.args.get('days', type=int)
        metal_type = request.args.get('metal_type')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        if metal_type and metal_type.lower() == 'all':
            metal_type = None

        history_data = get_price_history(days, metal_type, start_date, end_date)

        if not history_data:
            return jsonify({'status': 'error', 'message': 'Aucune donnée à exporter'}), 404

        # Préparation des dates (unique, triées)
        date_set = set()
        for item in history_data:
            pd = item['price_date']
            if isinstance(pd, datetime):
                pd = pd.date()
            date_set.add(pd)
        sorted_dates = sorted(list(date_set))

        # Pivot data: {metal: {currency, unit, prices{date: price}}}
        pivot_data = {}
        for item in history_data:
            metal = item['metal_type']
            pd = item['price_date']
            if isinstance(pd, datetime):
                pd = pd.date()
            price = item['price']
            currency = item['currency']
            unit = item['unit']

            if metal not in pivot_data:
                pivot_data[metal] = {
                    'currency': currency,
                    'unit': unit,
                    'prices': {}
                }
            pivot_data[metal]['prices'][pd] = price

        # Création du fichier Excel
        wb = Workbook()
        ws = wb.active
        ws.title = "Historique Prix Métaux"

        # Styles
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(start_color="0066B2", end_color="0066B2", fill_type="solid")
        data_fill = PatternFill(start_color="F5F7FA", end_color="F5F7FA", fill_type="solid")
        center_alignment = Alignment(horizontal='center', vertical='center')
        left_alignment = Alignment(horizontal='left', vertical='center')
        thin_border = Side(style='thin', color="CCCCCC")
        border = Border(top=thin_border, left=thin_border, right=thin_border, bottom=thin_border)

        # En-têtes
        headers = ['Produit (Metal)', 'Devise', 'Unité'] + [dt.strftime('%Y-%m-%d') for dt in sorted_dates]
        
        for col_idx, header in enumerate(headers, start=1):
            cell = ws.cell(row=1, column=col_idx, value=header)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = center_alignment
            cell.border = border

        # Remplissage des données
        row_idx = 2
        for metal, data in pivot_data.items():
            ws.cell(row=row_idx, column=1, value=metal).alignment = left_alignment
            ws.cell(row=row_idx, column=2, value=data['currency']).alignment = center_alignment
            ws.cell(row=row_idx, column=3, value=data['unit']).alignment = center_alignment
            
            for col in range(1, 4):
                cell = ws.cell(row=row_idx, column=col)
                cell.fill = data_fill
                cell.border = border

            col_idx = 4
            for dt in sorted_dates:
                price = data['prices'].get(dt)
                cell = ws.cell(row=row_idx, column=col_idx)
                
                if price is not None:
                    cell.value = float(price)
                    if float(price).is_integer():
                        cell.number_format = '#,##0'
                    else:
                        cell.number_format = '#,##0.########'
                else:
                    cell.value = ""

                cell.border = border
                cell.alignment = center_alignment
                col_idx += 1

            row_idx += 1

        # Ajuster la largeur des colonnes
        ws.column_dimensions['A'].width = 35
        ws.column_dimensions['B'].width = 10
        ws.column_dimensions['C'].width = 10
        for col_idx in range(4, len(sorted_dates) + 4):
            ws.column_dimensions[get_column_letter(col_idx)].width = 12

        # Sauvegarder dans un buffer
        output = BytesIO()
        wb.save(output)
        output.seek(0)

        filename = f"Prix_Metaux_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )

    except Exception as e:
        logger.error(f"Erreur export Excel: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

# ---------- API ECB / FX ----------
@app.route('/ecb/rates')
def api_ecb_rates():
    """
    API JSON pour les taux ECB (utilisée par la page 'Taux de change ECB').
    Paramètres (query string) :
      - start_date (YYYY-MM-DD, optionnel)
      - end_date   (YYYY-MM-DD, optionnel)
      - quote_currency (code devise, optionnel)
      - month (YYYY-MM, optionnel)
    """
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    quote_currency = request.args.get('quote_currency')
    month = request.args.get('month')  # NEW: Get month parameter

    rates = get_ecb_rates(
        start_date=start_date, 
        end_date=end_date, 
        quote_currency=quote_currency,
        month=month  # NEW: Pass month parameter
    )

    def serialize_rate(row):
        d = dict(row)
        if isinstance(d.get('ref_date'), (date, datetime)):
            d['ref_date'] = d['ref_date'].isoformat()
        if d.get('rate') is not None:
            d['rate'] = float(d['rate'])
        return d

    data = [serialize_rate(r) for r in rates]

    return jsonify({'status': 'success', 'data': data})

@app.route('/ecb/rates/export')
def api_ecb_rates_export():
    """
    Export Excel des taux ECB (non pivoté, tableau simple).
    Colonnes : Date, Base Currency, Quote Currency, Rate.
    """
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        quote_currency = request.args.get('quote_currency')
        month = request.args.get('month')  # NEW: Get month parameter

        rates = get_ecb_rates(
            start_date=start_date, 
            end_date=end_date, 
            quote_currency=quote_currency,
            month=month  # NEW: Pass month parameter
        )

        if not rates:
            return jsonify({'status': 'error', 'message': 'Aucun taux à exporter'}), 404

        wb = Workbook()
        ws = wb.active
        ws.title = "ECB FX Rates"

        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(start_color="0066B2", end_color="0066B2", fill_type="solid")
        center_alignment = Alignment(horizontal='center', vertical='center')
        thin_border = Side(style='thin', color="CCCCCC")
        border = Border(top=thin_border, left=thin_border, right=thin_border, bottom=thin_border)

        headers = ['Date', 'Base Currency', 'Quote Currency', 'Rate']
        for col_idx, header in enumerate(headers, start=1):
            cell = ws.cell(row=1, column=col_idx, value=header)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = center_alignment
            cell.border = border

        row_idx = 2
        for r in rates:
            ref_date = r['ref_date']
            if isinstance(ref_date, datetime):
                ref_date = ref_date.date()

            ws.cell(row=row_idx, column=1, value=ref_date)
            ws.cell(row=row_idx, column=2, value=r['base_currency'])
            ws.cell(row=row_idx, column=3, value=r['quote_currency'])

            rate_cell = ws.cell(row=row_idx, column=4)
            if r['rate'] is not None:
                rate_cell.value = float(r['rate'])
                rate_cell.number_format = '#,##0.0000'
            else:
                rate_cell.value = None

            for col_idx in range(1, 5):
                c = ws.cell(row=row_idx, column=col_idx)
                c.alignment = center_alignment
                c.border = border

            row_idx += 1

        ws.column_dimensions['A'].width = 12
        ws.column_dimensions['B'].width = 14
        ws.column_dimensions['C'].width = 16
        ws.column_dimensions['D'].width = 14

        output = BytesIO()
        wb.save(output)
        output.seek(0)

        filename = f"ECB_Rates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )

    except Exception as e:
        logger.error(f"Erreur export FX Excel: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500

@app.route('/ecb/export-florent')
def export_florent():
    """
    Export Excel du rapport mensuel FX pour Florent avec style AVO Carbon.
    Colonnes: Currency | Closing Rate (Mois) | Period Rate (M-1) | Average YTD
    """
    try:
        month = request.args.get('month', type=int)
        year = request.args.get('year', type=int)
        
        if not month or not year:
            return jsonify({'status': 'error', 'message': 'Paramètres month et year requis'}), 400
        
        month_name = datetime(year, month, 1).strftime('%B %Y')
        
        data = get_florent_report_data(year, month)
        
        if not data:
            return jsonify({'status': 'error', 'message': 'Aucune donnée disponible pour cette période'}), 404
        
        wb = Workbook()
        ws = wb.active
        ws.title = "Monthly FX Report"
        
        # --- STYLING CONFIGURATION ---
        header_fill = PatternFill(start_color="002060", end_color="002060", fill_type="solid")  # Navy Blue
        header_font = Font(color="FFFFFF", bold=True, size=12)
        border_style = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        center_align = Alignment(horizontal='center', vertical='center')
        right_align = Alignment(horizontal='right', vertical='center')
        
        # 1. Write Headers
        headers = ['Currency', f'Closing Rate ({month_name})', 'Period Rate', f'Average YTD {month_name}{year}']
        ws.append(headers)
        
        # Apply Header Styles
        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = center_align
            cell.border = border_style
        
        # 2. Write Data Rows
        for row_data in data:
            row = [
                row_data['quote_currency'],
                row_data['closing_rate'] if row_data['closing_rate'] is not None else None,
                row_data['period_rate'] if row_data['period_rate'] is not None else "N/A",
                row_data['ytd_average'] if row_data['ytd_average'] is not None else None
            ]
            ws.append(row)
        
        # 3. Format Data Cells (Borders & Numbers)
        for row in ws.iter_rows(min_row=2):
            for idx, cell in enumerate(row):
                cell.border = border_style
                if idx == 0:  # Currency column
                    cell.alignment = center_align
                else:  # Numeric columns
                    if isinstance(cell.value, (float, int)):
                        cell.number_format = '0.0000'  # 4 decimals
                        cell.alignment = right_align
                    elif cell.value == "N/A":
                        cell.alignment = center_align
        
        # Adjust Column Widths
        ws.column_dimensions['A'].width = 15
        ws.column_dimensions['B'].width = 22
        ws.column_dimensions['C'].width = 22
        ws.column_dimensions['D'].width = 22
        
        # Save to memory and return
        output = BytesIO()
        wb.save(output)
        output.seek(0)
        
        filename = f"AVO_Monthly_FX_{month:02d}_{year}.xlsx"
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        logger.error(f"Erreur export Florent: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
# ===============================
# POINT D'ENTRÉE
# ===============================
if __name__ == '__main__':
    app.run(debug=True, port=5000)
