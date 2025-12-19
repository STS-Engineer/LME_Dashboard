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
# PARTIE ECB / TAUX DE CHANGE
# ===============================
def get_ecb_rates(start_date=None, end_date=None, quote_currency=None):
    """
    Récupère les taux de change ECB depuis la table ecb_exchange_rates.
    Filtres:
      - start_date, end_date : 'YYYY-MM-DD' (optionnels)
      - quote_currency : code devise (ex: 'USD') ou None pour toutes.
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

            if quote_currency and quote_currency.lower() != 'all':
                query += " AND quote_currency = %s"
                params.append(quote_currency.upper())

            # Si aucune date fournie, on limite par défaut aux 365 derniers jours
            if not start_date and not end_date:
                query += " AND ref_date >= %s"
                params.append(datetime.now().date() - timedelta(days=365))

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
    """API pour l'historique des prix, avec filtres."""
    days = request.args.get('days', type=int)
    metal_type = request.args.get('metal_type')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if metal_type and metal_type.lower() == 'all':
        metal_type = None

    history = get_price_history(days, metal_type, start_date, end_date)
    
    # Sérialiser les objets date pour JSON
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
    """
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    quote_currency = request.args.get('quote_currency')

    rates = get_ecb_rates(start_date=start_date, end_date=end_date, quote_currency=quote_currency)

    def serialize_rate(row):
        d = dict(row)
        if isinstance(d.get('ref_date'), (date, datetime)):
            d['ref_date'] = d['ref_date'].isoformat()
        if d.get('rate') is not None:
            d['rate'] = float(d['rate'])
        # metadata peut être jsonb -> le laisser tel quel ou le cast en str si nécessaire
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

        rates = get_ecb_rates(start_date=start_date, end_date=end_date, quote_currency=quote_currency)

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

# ===============================
# POINT D'ENTRÉE
# ===============================
if __name__ == '__main__':
    app.run(debug=True, port=5000)
