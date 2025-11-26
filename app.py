# app.py - Dashboard AVO Carbon Group
"""
Dashboard Flask pour visualiser les prix des métaux et gérer l'export Excel.
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
# FONCTIONS DATABASE
# ===============================
def get_db_connection():
    """Créer une connexion à PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Erreur de connexion à la base de données: {e}")
        return None

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
    CORRECTION: Ajout de 'source_url' dans le SELECT.
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

            # Filtrage par date de début (start_date)
            if start_date:
                query += " AND price_date >= %s"
                params.append(start_date)
            
            # Filtrage par date de fin (end_date)
            if end_date:
                query += " AND price_date <= %s"
                end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
                params.append(str(end_date_obj + timedelta(days=1)))
            
            # Filtrage par nombre de jours (si pas de dates spécifiques)
            elif days:
                start_date_obj = datetime.now() - timedelta(days=int(days))
                query += " AND price_date >= %s"
                params.append(start_date_obj.strftime('%Y-%m-%d'))
            
            # Filtrage par type de métal
            if metal_type and metal_type.lower() != 'all':
                query += " AND metal_type = %s"
                params.append(metal_type)

            # Ordonner par type de métal puis par date de prix la plus récente
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

            # 2. Calcul de la variation 24h
            query_variation = """
                WITH RankedPrices AS (
                    SELECT 
                        id,
                        metal_type, 
                        price, 
                        currency,
                        price_date,
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
                    SELECT DISTINCT ON (metal_type)
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
    """Récupère les logs de synchronisation."""
    conn = get_db_connection()
    if not conn:
        return []

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = f"""
                SELECT 
                    id, 
                    sync_start_time, 
                    sync_end_time, 
                    status, 
                    details 
                FROM 
                    sync_logs
                ORDER BY 
                    sync_start_time DESC
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
        if log_dict.get('sync_start_time'):
            log_dict['sync_start_time'] = log_dict['sync_start_time'].isoformat()
        if log_dict.get('sync_end_time'):
            log_dict['sync_end_time'] = log_dict['sync_end_time'].isoformat()
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

        history_data = get_price_history(days, metal_type, start_date, end_date)

        if not history_data:
            return jsonify({'status': 'error', 'message': 'Aucune donnée à exporter'}), 404

        # Préparation des données pour le Pivot
        date_set = set((item['price_date'].date() if isinstance(item['price_date'], datetime) else item['price_date'])
    for item in history_data)
        sorted_dates = sorted(list(date_set))

        pivot_data = {}
        for item in history_data:
            metal = item['metal_type']
            price_date = item['price_date']
            price = item['price']
            currency = item['currency']
            unit = item['unit']

            if metal not in pivot_data:
                pivot_data[metal] = {
                    'currency': currency,
                    'unit': unit,
                    'prices': {}
                }
            pivot_data[metal]['prices'][price_date] = price

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
        
        col_idx = 1
        for header in headers:
            cell = ws.cell(row=1, column=col_idx, value=header)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = center_alignment
            cell.border = border
            col_idx += 1

        # Remplissage des données
        row_idx = 2
        for metal, data in pivot_data.items():
            ws.cell(row=row_idx, column=1, value=metal).alignment = left_alignment
            ws.cell(row=row_idx, column=2, value=data['currency']).alignment = center_alignment
            ws.cell(row=row_idx, column=3, value=data['unit']).alignment = center_alignment
            
            for col in range(1, 4):
                 ws.cell(row=row_idx, column=col).fill = data_fill
                 ws.cell(row=row_idx, column=col).border = border

            col_idx = 4
            for dt in sorted_dates:
                price = data['prices'].get(dt)
                cell = ws.cell(row=row_idx, column=col_idx)
                
                if price is not None:
                    cell.value = price
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
# ===============================
# POINT D'ENTRÉE
# ===============================
if __name__ == '__main__':
    app.run(debug=True, port=5000)
