# app.py - Dashboard AVO Carbon Group
"""
Dashboard Flask pour visualiser les prix des métaux
"""
from flask import Flask, render_template, jsonify, request, send_file
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
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

# ==============================
# CONFIGURATION
# ==============================
app = Flask(__name__)
app.config['SECRET_KEY'] = 'avo-carbon-dashboard-2025'

# Configuration base de données
DB_CONFIG = {
    "user": "administrationSTS",
    "password": "St$@0987",
    "host": "avo-adb-002.postgres.database.azure.com",
    "port": "5432",
    "database": "LME_DB",
    "sslmode": "require"
}

# ==============================
# FONCTIONS DATABASE
# ==============================
def get_db_connection():
    """Créer une connexion à PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Erreur connexion DB: {e}")
        raise


def get_latest_prices():
    """Récupérer les derniers prix pour chaque métal."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT DISTINCT ON (metal_type)
                id,
                source_product_name,
                metal_type,
                price,
                currency,
                unit,
                created_at
            FROM metal_prices
            ORDER BY metal_type, created_at DESC
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return results
    except Exception as e:
        logger.error(f"Erreur get_latest_prices: {e}")
        return []


def get_price_history(days=7, metal_type=None, start_date=None, end_date=None):
    """Récupérer l'historique des prix avec filtres."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Construction de la requête avec filtres
        query = """
            SELECT 
                id,
                source_product_name,
                metal_type,
                price,
                currency,
                unit,
                created_at
            FROM metal_prices
            WHERE 1=1
        """
        params = []
        
        # Filtre par type de métal
        if metal_type and metal_type != 'all':
            query += " AND metal_type = %s"
            params.append(metal_type)
        
        # Filtre par dates
        if start_date:
            query += " AND created_at >= %s"
            params.append(start_date)
        
        if end_date:
            query += " AND created_at <= %s"
            params.append(end_date)
        
        # Si pas de dates spécifiques, utiliser le paramètre days
        if not start_date and not end_date:
            query += " AND created_at >= NOW() - INTERVAL '%s days'"
            params.append(days)
        
        query += " ORDER BY created_at DESC LIMIT 500"
        
        cursor.execute(query, tuple(params))
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return results
    except Exception as e:
        logger.error(f"Erreur get_price_history: {e}")
        return []


def get_sync_logs(limit=10):
    """Récupérer les logs de synchronisation."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT 
                id,
                sync_type,
                status,
                metals_updated,
                error_message,
                duration_seconds,
                created_at
            FROM sync_logs
            ORDER BY created_at DESC
            LIMIT %s
        """
        
        cursor.execute(query, (limit,))
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return results
    except Exception as e:
        logger.error(f"Erreur get_sync_logs: {e}")
        return []


def get_statistics():
    """Calculer des statistiques sur les prix."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Stats globales
        query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT metal_type) as total_metals,
                MIN(created_at) as first_record,
                MAX(created_at) as last_update
            FROM metal_prices
        """
        
        cursor.execute(query)
        stats = cursor.fetchone()
        
        # Variation 24h pour chaque métal
        query_variations = """
            WITH latest AS (
                SELECT DISTINCT ON (metal_type)
                    metal_type,
                    price as current_price,
                    created_at
                FROM metal_prices
                ORDER BY metal_type, created_at DESC
            ),
            previous AS (
                SELECT DISTINCT ON (metal_type)
                    metal_type,
                    price as previous_price
                FROM metal_prices
                WHERE created_at <= NOW() - INTERVAL '1 day'
                ORDER BY metal_type, created_at DESC
            )
            SELECT 
                l.metal_type,
                l.current_price,
                p.previous_price,
                CASE 
                    WHEN p.previous_price IS NOT NULL AND p.previous_price > 0
                    THEN ROUND(((l.current_price - p.previous_price) / p.previous_price * 100)::numeric, 2)
                    ELSE 0
                END as variation_percent
            FROM latest l
            LEFT JOIN previous p ON l.metal_type = p.metal_type
        """
        
        cursor.execute(query_variations)
        variations = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        stats['variations'] = variations
        
        return stats
    except Exception as e:
        logger.error(f"Erreur get_statistics: {e}")
        return {}


# ==============================
# ROUTES
# ==============================
@app.route('/')
def index():
    """Page principale du dashboard."""
    return render_template('index.html')


@app.route('/api/prices/latest')
def api_latest_prices():
    """API: Derniers prix."""
    prices = get_latest_prices()
    return jsonify({
        'status': 'success',
        'data': prices,
        'count': len(prices)
    })


@app.route('/api/prices/history')
def api_price_history():
    """API: Historique des prix avec filtres."""
    days = request.args.get('days', default=7, type=int)
    metal_type = request.args.get('metal_type', default=None)
    start_date = request.args.get('start_date', default=None)
    end_date = request.args.get('end_date', default=None)
    
    history = get_price_history(days, metal_type, start_date, end_date)
    return jsonify({
        'status': 'success',
        'data': history,
        'count': len(history)
    })


@app.route('/api/sync/logs')
def api_sync_logs():
    """API: Logs de synchronisation."""
    limit = request.args.get('limit', default=10, type=int)
    logs = get_sync_logs(limit)
    return jsonify({
        'status': 'success',
        'data': logs,
        'count': len(logs)
    })


@app.route('/api/statistics')
def api_statistics():
    """API: Statistiques."""
    stats = get_statistics()
    return jsonify({
        'status': 'success',
        'data': stats
    })


@app.route('/health')
def health():
    """Health check."""
    db_status = 'disconnected'
    try:
        conn = get_db_connection()
        conn.close()
        db_status = 'connected'
    except:
        pass
    
    return jsonify({
        'status': 'healthy',
        'database': db_status,
        'timestamp': datetime.now().isoformat()
    })


@app.route('/export/excel')
def export_excel():
    """Exporter les données en Excel avec le format demandé."""
    try:
        # Récupérer les filtres
        metal_type = request.args.get('metal_type', default=None)
        start_date = request.args.get('start_date', default=None)
        end_date = request.args.get('end_date', default=None)
        days = request.args.get('days', default=30, type=int)
        
        # Récupérer les données
        data = get_price_history(days, metal_type, start_date, end_date)
        
        if not data:
            return jsonify({'error': 'Aucune donnée à exporter'}), 404
        
        # Créer le workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "Prix des Métaux"
        
        # Organiser les données par date (colonnes) et par produit (lignes)
        # Format: Date en colonnes, Produits en lignes
        
        # Extraire les dates uniques et les trier
        dates = {}
        products = set()
        
        for row in data:
            date_key = row['created_at'].strftime('%d/%m')
            products.add(row['source_product_name'])
            
            if date_key not in dates:
                dates[date_key] = {}
            
            dates[date_key][row['source_product_name']] = row['price']
        
        # Trier les dates
        sorted_dates = sorted(dates.keys(), key=lambda x: datetime.strptime(x + '/2025', '%d/%m/%Y'))
        sorted_products = sorted(list(products))
        
        # Style pour les en-têtes
        header_fill = PatternFill(start_color="FFC000", end_color="FFC000", fill_type="solid")
        header_font = Font(bold=True, size=11)
        border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        center_alignment = Alignment(horizontal='center', vertical='center')
        
        # En-tête des colonnes (dates)
        ws['A1'] = "Produit / Date"
        ws['A1'].fill = header_fill
        ws['A1'].font = header_font
        ws['A1'].border = border
        ws['A1'].alignment = center_alignment
        
        col_idx = 2
        for date_str in sorted_dates:
            cell = ws.cell(row=1, column=col_idx)
            cell.value = date_str
            cell.fill = header_fill
            cell.font = header_font
            cell.border = border
            cell.alignment = center_alignment
            col_idx += 1
        
        # Lignes des produits
        row_idx = 2
        product_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
        
        for product in sorted_products:
            # Nom du produit dans la première colonne
            cell = ws.cell(row=row_idx, column=1)
            cell.value = product
            cell.fill = product_fill
            cell.font = Font(bold=True, size=10)
            cell.border = border
            
            # Prix pour chaque date
            col_idx = 2
            for date_str in sorted_dates:
                cell = ws.cell(row=row_idx, column=col_idx)
                
                if product in dates[date_str]:
                    cell.value = dates[date_str][product]
                    cell.number_format = '#,##0.00'
                else:
                    cell.value = ""
                
                cell.border = border
                cell.alignment = center_alignment
                col_idx += 1
            
            row_idx += 1
        
        # Ajuster la largeur des colonnes
        ws.column_dimensions['A'].width = 35
        for col_idx in range(2, len(sorted_dates) + 2):
            ws.column_dimensions[get_column_letter(col_idx)].width = 12
        
        # Sauvegarder dans un buffer
        output = BytesIO()
        wb.save(output)
        output.seek(0)
        
        # Nom du fichier avec timestamp
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


# ==============================
# POINT D'ENTRÉE
# ==============================
if __name__ == '__main__':
    logger.info("="*80)
    logger.info("🚀 DÉMARRAGE DASHBOARD AVO CARBON GROUP")
    logger.info("📊 Dashboard disponible sur: http://localhost:5001")
    logger.info("="*80)
    
    app.run(
        host='0.0.0.0',
        port=5001,
        debug=True
    )