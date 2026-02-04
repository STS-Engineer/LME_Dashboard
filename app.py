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
import os
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
# ==============================
# IMPORTS SUPPLÉMENTAIRES (ajouter en haut du fichier)
# ==============================
from flask_mail import Mail, Message
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import atexit
import secrets
from datetime import datetime, timedelta

# ==============================
# CONFIGURATION EMAIL (ton code existant)
# ==============================
app.config['MAIL_SERVER'] = 'avocarbon-com.mail.protection.outlook.com'
app.config['MAIL_PORT'] = 25
app.config['MAIL_USE_TLS'] = False
app.config['MAIL_USE_SSL'] = False
app.config['MAIL_USERNAME'] = None
app.config['MAIL_PASSWORD'] = None
app.config['MAIL_DEFAULT_SENDER'] = 'administration.STS@avocarbon.com'

# Initialiser Flask-Mail
mail = Mail(app)

# ==============================
# CONFIGURATION BUDGET RATE
# ==============================
BUDGET_OWNER_EMAIL = "marjana.delija@avocarbon.com"  # ✅ À MODIFIER
BUDGET_CURRENCIES = ['USD', 'CNY','INR','KRW','MXN','TND']

# Stockage temporaire des tokens (en production, utiliser Redis ou DB)
active_tokens = {}

# ==============================
# FONCTION: Générer un token sécurisé
# ==============================
def generate_secure_token(year):
    """Génère un token unique pour sécuriser le formulaire"""
    token = secrets.token_urlsafe(32)
    active_tokens[token] = {
        'year': year,
        'created_at': datetime.now(),
        'used': False
    }
    logger.info(f"✅ Token créé: {token} pour l'année {year}")
    return token

# ==============================
# FONCTION: Template HTML Email
# ==============================
def get_email_html_template(year, form_url, token):
    """Retourne le HTML de l'email avec formulaire intégré"""
    return f"""
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Budget FX Rates {year}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            margin: 0;
            padding: 20px;
        }}
        .container {{
            max-width: 800px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            overflow: hidden;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
        }}
        .header {{
            background: linear-gradient(135deg, #0066b2 0%, #004d8c 100%);
            color: white;
            padding: 40px 30px;
            text-align: center;
        }}
        .header h1 {{
            margin: 0;
            font-size: 32px;
            font-weight: 700;
        }}
        .header p {{
            margin: 10px 0 0 0;
            font-size: 16px;
            opacity: 0.9;
        }}
        .content {{
            padding: 40px 30px;
        }}
        .alert {{
            background: #fff3cd;
            border-left: 4px solid #f39c12;
            padding: 15px;
            margin-bottom: 30px;
            border-radius: 5px;
        }}
        .alert strong {{
            color: #856404;
        }}
        .info-box {{
            background: #e8f4fd;
            border-left: 4px solid #0066b2;
            padding: 20px;
            margin: 20px 0;
            border-radius: 5px;
        }}
        .btn {{
            display: inline-block;
            background: linear-gradient(135deg, #27ae60 0%, #229954 100%);
            color: white;
            text-decoration: none;
            padding: 15px 40px;
            border-radius: 8px;
            font-size: 16px;
            font-weight: 600;
            text-align: center;
            margin: 20px 0;
            box-shadow: 0 4px 15px rgba(39, 174, 96, 0.3);
            transition: all 0.3s ease;
        }}
        .btn:hover {{
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(39, 174, 96, 0.4);
        }}
        .currencies {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(100px, 1fr));
            gap: 10px;
            margin: 20px 0;
        }}
        .currency-badge {{
            background: #f5f7fa;
            padding: 10px;
            text-align: center;
            border-radius: 8px;
            font-weight: 600;
            color: #2c3e50;
            border: 2px solid #ecf0f1;
        }}
        .footer {{
            background: #f8f9fa;
            padding: 20px 30px;
            text-align: center;
            color: #7f8c8d;
            font-size: 14px;
        }}
        .deadline {{
            background: #e74c3c;
            color: white;
            padding: 15px;
            border-radius: 8px;
            text-align: center;
            margin: 20px 0;
            font-weight: 600;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🎯 Budget FX Rates {year}</h1>
            <p>Formulaire de saisie annuel</p>
        </div>
        
        <div class="content">
            <div class="alert">
                <strong>⚠️ Action requise</strong><br>
                Bonjour Florent,<br><br>
                Il est temps de définir les <strong>Budget Rates</strong> pour l'année <strong>{year}</strong>.
                Ces taux seront utilisés dans les rapports mensuels FX.
            </div>
            
            <div class="info-box">
                <h3 style="margin-top: 0; color: #0066b2;">📋 Devises à remplir ({len(BUDGET_CURRENCIES)})</h3>
                <div class="currencies">
                    {''.join([f'<div class="currency-badge">{curr}</div>' for curr in BUDGET_CURRENCIES])}
                </div>
            </div>
            
            <div class="deadline">
                ⏰ Date limite : 31 Décembre {year - 1}
            </div>
            
            <div style="text-align: center;">
                <a href="{form_url}" class="btn">
                    ✏️ Remplir le formulaire Budget {year}
                </a>
            </div>
            
            <div style="margin-top: 30px; padding: 20px; background: #f8f9fa; border-radius: 8px;">
                <h4 style="margin-top: 0; color: #2c3e50;">ℹ️ Informations importantes</h4>
                <ul style="color: #555; line-height: 1.8;">
                    <li><strong>Lien valide :</strong> 30 jours</li>
                    <li><strong>Format :</strong> Utiliser le point comme séparateur décimal (ex: 1.10)</li>
                    <li><strong>Modification :</strong> Le formulaire peut être modifié avant validation finale</li>
                    <li><strong>Sécurité :</strong> Ce lien est unique et personnel</li>
                </ul>
            </div>
            
            <div style="margin-top: 20px; padding: 15px; border-top: 2px solid #ecf0f1;">
                <p style="margin: 0; color: #7f8c8d; font-size: 13px;">
                    <strong>Token :</strong> <code style="background: #f5f7fa; padding: 5px 10px; border-radius: 4px;">{token}</code><br>
                    <strong>URL :</strong> {form_url}
                </p>
            </div>
        </div>
        
        <div class="footer">
            <p style="margin: 5px 0;">
                <strong>AVO Carbon Group</strong> - Dashboard FX
            </p>
            <p style="margin: 5px 0; font-size: 12px;">
                Cet email a été généré automatiquement. Ne pas répondre.
            </p>
            <p style="margin: 5px 0; font-size: 12px;">
                Pour toute question : <a href="mailto:administration.STS@avocarbon.com">administration.STS@avocarbon.com</a>
            </p>
        </div>
    </div>
</body>
</html>
"""

# ==============================
# FONCTION: Template HTML Formulaire
# ==============================
def get_form_html_template(year, token, existing_rates=None):
    """Retourne le HTML du formulaire de saisie"""
    existing_rates = existing_rates or {}
    
    currency_inputs = ""
    for curr in BUDGET_CURRENCIES:
        value = existing_rates.get(curr, "")
        currency_inputs += f"""
        <div class="currency-input">
            <label for="{curr}">{curr}</label>
            <input 
                type="number" 
                id="{curr}" 
                name="{curr}" 
                step="0.0001" 
                placeholder="1.1000"
                value="{value}"
                required
            >
        </div>
        """
    
    return f"""
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Budget FX Rates {year}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: 'Segoe UI', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        .container {{
            max-width: 900px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #0066b2 0%, #004d8c 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        .header h1 {{ margin: 0; font-size: 28px; }}
        .content {{ padding: 40px 30px; }}
        .currency-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
            gap: 20px;
            margin: 30px 0;
        }}
        .currency-input {{
            display: flex;
            flex-direction: column;
            gap: 8px;
        }}
        .currency-input label {{
            font-weight: 600;
            color: #2c3e50;
            font-size: 14px;
            text-transform: uppercase;
        }}
        .currency-input input {{
            padding: 12px 15px;
            border: 2px solid #ecf0f1;
            border-radius: 8px;
            font-size: 16px;
            transition: all 0.3s ease;
        }}
        .currency-input input:focus {{
            outline: none;
            border-color: #0066b2;
            box-shadow: 0 0 0 4px rgba(0, 102, 178, 0.1);
        }}
        .btn-submit {{
            width: 100%;
            background: linear-gradient(135deg, #27ae60 0%, #229954 100%);
            color: white;
            border: none;
            padding: 18px;
            font-size: 18px;
            font-weight: 600;
            border-radius: 10px;
            cursor: pointer;
            margin-top: 30px;
            transition: all 0.3s ease;
        }}
        .btn-submit:hover {{
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(39, 174, 96, 0.4);
        }}
        .btn-submit:disabled {{
            background: #95a5a6;
            cursor: not-allowed;
            transform: none;
        }}
        .alert {{
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 20px;
        }}
        .alert-info {{
            background: #e8f4fd;
            border-left: 4px solid #0066b2;
            color: #004d8c;
        }}
        .alert-success {{
            background: #d4edda;
            border-left: 4px solid #27ae60;
            color: #155724;
        }}
        .alert-error {{
            background: #f8d7da;
            border-left: 4px solid #e74c3c;
            color: #721c24;
        }}
        #message {{ display: none; }}
        .spinner {{
            border: 3px solid #f3f3f3;
            border-top: 3px solid #0066b2;
            border-radius: 50%;
            width: 20px;
            height: 20px;
            animation: spin 1s linear infinite;
            display: inline-block;
            margin-right: 10px;
        }}
        @keyframes spin {{
            0% {{ transform: rotate(0deg); }}
            100% {{ transform: rotate(360deg); }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Budget FX Rates {year}</h1>
            <p>Formulaire de saisie des taux budgétaires</p>
        </div>
        
        <div class="content">
            <div class="alert alert-info">
                <strong>ℹ️ Instructions</strong><br>
                Saisissez les taux budgétaires pour chaque devise (EUR → Devise).<br>
                Format : utiliser le point comme séparateur décimal (ex: 1.1000)
            </div>
            
            <div id="message"></div>
            
            <form id="budgetForm">
                <div class="currency-grid">
                    {currency_inputs}
                </div>
                
                <button type="submit" class="btn-submit" id="submitBtn">
                    ✅ Enregistrer les Budget Rates {year}
                </button>
            </form>
        </div>
    </div>
    
    <script>
        const form = document.getElementById('budgetForm');
        const messageDiv = document.getElementById('message');
        const submitBtn = document.getElementById('submitBtn');
        
        form.addEventListener('submit', async (e) => {{
            e.preventDefault();
            
            // Désactiver le bouton
            submitBtn.disabled = true;
            submitBtn.innerHTML = '<span class="spinner"></span>Enregistrement en cours...';
            
            // Collecter les données
            const formData = new FormData(form);
            const rates = {{}};
            
            for (let [key, value] of formData.entries()) {{
                rates[key] = parseFloat(value);
            }}
            
            // Envoyer au serveur
            try {{
                const response = await fetch('/api/submit-budget-rates', {{
                    method: 'POST',
                    headers: {{
                        'Content-Type': 'application/json'
                    }},
                    body: JSON.stringify({{
                        token: '{token}',
                        year: {year},
                        rates: rates
                    }})
                }});
                
                const result = await response.json();
                
                if (result.status === 'success') {{
                    messageDiv.className = 'alert alert-success';
                    messageDiv.innerHTML = '<strong>✅ Succès !</strong><br>' + result.message;
                    messageDiv.style.display = 'block';
                    
                    // Désactiver le formulaire
                    form.querySelectorAll('input').forEach(input => input.disabled = true);
                    submitBtn.innerHTML = '✅ Budget Rates enregistrés';
                }} else {{
                    throw new Error(result.message || 'Erreur inconnue');
                }}
                
            }} catch (error) {{
                messageDiv.className = 'alert alert-error';
                messageDiv.innerHTML = '<strong>❌ Erreur</strong><br>' + error.message;
                messageDiv.style.display = 'block';
                
                submitBtn.disabled = false;
                submitBtn.innerHTML = '✅ Enregistrer les Budget Rates {year}';
            }}
        }});
    </script>
</body>
</html>
"""

# ==============================
# FONCTION: Envoyer l'email Budget Rate
# ==============================
def send_budget_rate_email(year, recipient_email=BUDGET_OWNER_EMAIL, test_mode=False):
    """
    Envoie un email à Florent avec le formulaire Budget Rate
    
    Args:
        year: Année pour laquelle définir les budgets
        recipient_email: Email du destinataire
        test_mode: Si True, affiche l'URL au lieu d'envoyer l'email
    """
    try:
        # Générer un token de sécurité
        token = generate_secure_token(year)
        # URL du formulaire (adapter selon ton environnement)
        # ✅ EN PRODUCTION, REMPLACER PAR TON VRAI DOMAINE
        if test_mode:
            form_url = f"http://localhost:5000/budget-form/{token}"
        else:
            form_url = f"https://avo-exmetrics.azurewebsites.net/budget-form/{token}"
        
        # Générer le HTML de l'email
        email_html = get_email_html_template(year, form_url, token)
        
        if test_mode:
            logger.info("=" * 60)
            logger.info("🧪 MODE TEST - Email non envoyé")
            logger.info(f"📧 Destinataire : {recipient_email}")
            logger.info(f"📅 Année : {year}")
            logger.info(f"🔗 URL du formulaire : {form_url}")
            logger.info(f"🔑 Token : {token}")
            logger.info("=" * 60)
            return True
        
        # Créer le message email
        msg = Message(
            subject=f"[ACTION REQUISE] Budget FX Rates {year}",
            recipients=[recipient_email],
            html=email_html
        )
        
        # Envoyer l'email
        mail.send(msg)
        
        logger.info(f"✅ Email Budget Rate {year} envoyé à {recipient_email}")
        logger.info(f"🔗 URL du formulaire : {form_url}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur envoi email Budget Rate: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

# ==============================
# ROUTE: Afficher le formulaire Budget Rate
# ==============================
@app.route('/budget-form/<token>')
def budget_form(token):
    """Affiche le formulaire de saisie des Budget Rates"""
    
    # Vérifier le token
    if token not in active_tokens:
        return """
        <html>
            <body style="font-family: Arial; text-align: center; padding: 50px; background: #f5f7fa;">
                <div style="background: white; padding: 40px; border-radius: 15px; max-width: 600px; margin: 0 auto; box-shadow: 0 4px 20px rgba(0,0,0,0.1);">
                    <h1 style="color: #e74c3c;">❌ Lien invalide ou expiré</h1>
                    <p style="color: #7f8c8d;">Ce lien de formulaire n'est plus valide.</p>
                    <p style="color: #7f8c8d;">Contactez l'administrateur si vous pensez qu'il s'agit d'une erreur.</p>
                    <p style="margin-top: 30px;">
                        <a href="mailto:administration.STS@avocarbon.com" style="color: #0066b2; text-decoration: none;">
                            📧 Contacter l'administrateur
                        </a>
                    </p>
                </div>
            </body>
        </html>
        """, 403
    
    token_data = active_tokens[token]
    
    # Vérifier si déjà utilisé
    if token_data['used']:
        return """
        <html>
            <body style="font-family: Arial; text-align: center; padding: 50px; background: #f5f7fa;">
                <div style="background: white; padding: 40px; border-radius: 15px; max-width: 600px; margin: 0 auto; box-shadow: 0 4px 20px rgba(0,0,0,0.1);">
                    <h1 style="color: #27ae60;">✅ Formulaire déjà soumis</h1>
                    <p style="color: #7f8c8d;">Ce formulaire a déjà été complété.</p>
                    <p style="color: #7f8c8d;">Les Budget Rates ont été enregistrés avec succès.</p>
                </div>
            </body>
        </html>
        """, 200
    
    # Vérifier l'expiration (30 jours)
    age_days = (datetime.now() - token_data['created_at']).days
    if age_days > 30:
        return """
        <html>
            <body style="font-family: Arial; text-align: center; padding: 50px; background: #f5f7fa;">
                <div style="background: white; padding: 40px; border-radius: 15px; max-width: 600px; margin: 0 auto; box-shadow: 0 4px 20px rgba(0,0,0,0.1);">
                    <h1 style="color: #e74c3c;">⏰ Lien expiré</h1>
                    <p style="color: #7f8c8d;">Ce lien a expiré (valide 30 jours).</p>
                    <p style="color: #7f8c8d;">Contactez l'administrateur pour recevoir un nouveau lien.</p>
                </div>
            </body>
        </html>
        """, 410
    
    year = token_data['year']
    
    # Récupérer les valeurs existantes (si modification)
    existing_rates = {}
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT currency, budget_rate 
                    FROM fx_budget_rates 
                    WHERE year = %s
                """, (year,))
                rows = cur.fetchall()
                for row in rows:
                    existing_rates[row['currency']] = float(row['budget_rate'])
        except Exception as e:
            logger.error(f"Erreur récupération taux existants: {e}")
        finally:
            conn.close()
    
    # Retourner le HTML du formulaire
    return get_form_html_template(year, token, existing_rates)

# ==============================
# ROUTE: Soumettre le formulaire Budget Rate
# ==============================
@app.route('/api/submit-budget-rates', methods=['POST'])
def submit_budget_rates():
    """API pour recevoir et enregistrer les Budget Rates"""
    try:
        data = request.json
        token = data.get('token')
        year = data.get('year')
        rates = data.get('rates', {})  # {"USD": 1.10, "CNY": 7.90, ...}
        
        # Validation du token
        if token not in active_tokens:
            return jsonify({
                'status': 'error',
                'message': 'Token invalide ou expiré'
            }), 403
        
        token_data = active_tokens[token]
        
        # Vérifier si déjà utilisé
        if token_data['used']:
            return jsonify({
                'status': 'error',
                'message': 'Ce formulaire a déjà été soumis'
            }), 400
        
        # Vérifier l'expiration
        age_days = (datetime.now() - token_data['created_at']).days
        if age_days > 30:
            return jsonify({
                'status': 'error',
                'message': 'Le lien a expiré (valide 30 jours)'
            }), 410
        
        # Vérifier l'année
        if year != token_data['year']:
            return jsonify({
                'status': 'error',
                'message': 'Année invalide'
            }), 400
        
        # Validation des taux
        if not rates:
            return jsonify({
                'status': 'error',
                'message': 'Aucun taux fourni'
            }), 400
        
        for currency, rate in rates.items():
            if currency not in BUDGET_CURRENCIES:
                return jsonify({
                    'status': 'error',
                    'message': f'Devise invalide: {currency}'
                }), 400
            
            if not isinstance(rate, (int, float)) or rate <= 0:
                return jsonify({
                    'status': 'error',
                    'message': f'Taux invalide pour {currency}: {rate}'
                }), 400
        
        # Enregistrer dans la base de données
        conn = get_db_connection()
        if not conn:
            return jsonify({
                'status': 'error',
                'message': 'Erreur de connexion à la base de données'
            }), 500
        
        try:
            with conn.cursor() as cur:
                # Supprimer les taux existants pour cette année
                cur.execute("""
                    DELETE FROM fx_budget_rates 
                    WHERE year = %s
                """, (year,))
                
                # Insérer les nouveaux taux
                for currency, rate in rates.items():
                    cur.execute("""
                        INSERT INTO fx_budget_rates (year, currency, budget_rate, updated_at)
                        VALUES (%s, %s, %s, NOW())
                    """, (year, currency, rate))
                
                conn.commit()
                
                # Marquer le token comme utilisé
                active_tokens[token]['used'] = True
                
                logger.info(f"✅ Budget Rates {year} enregistrés avec succès: {len(rates)} devises")
                
                return jsonify({
                    'status': 'success',
                    'message': f'Budget Rates {year} enregistrés avec succès ({len(rates)} devises)',
                    'data': {
                        'year': year,
                        'currencies_count': len(rates)
                    }
                })
                
        except Exception as e:
            conn.rollback()
            logger.error(f"Erreur insertion Budget Rates: {e}")
            import traceback
            logger.error(traceback.format_exc())
            
            return jsonify({
                'status': 'error',
                'message': f'Erreur lors de l\'enregistrement: {str(e)}'
            }), 500
            
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Erreur API submit-budget-rates: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        return jsonify({
            'status': 'error',
            'message': f'Erreur serveur: {str(e)}'
        }), 500

# ==============================
# CRON JOB: Envoi automatique annuel
# ==============================
def scheduled_budget_email_job():
    """
    Job APScheduler : envoi automatique Budget Rate
    """

    with app.app_context():  # ✅ CONTEXTE FLASK OBLIGATOIRE

        try:
            current_year = datetime.now().year
            next_year = current_year + 1

            logger.info(f"🤖 Cron job déclenché: Envoi email Budget Rate {next_year}")

            success = send_budget_rate_email(
                year=next_year,
                recipient_email=BUDGET_OWNER_EMAIL,
                test_mode=False
            )

            if success:
                logger.info(f"✅ Email Budget Rate {next_year} envoyé avec succès")
            else:
                logger.error(f"❌ Échec envoi email Budget Rate {next_year}")

        except Exception as e:
            logger.error(f"❌ Erreur dans cron job Budget Rate: {e}")
            import traceback
            logger.error(traceback.format_exc())

# Initialiser le scheduler
scheduler = BackgroundScheduler()

scheduler.add_job(
    func=scheduled_budget_email_job,
    trigger=CronTrigger(month=11, day=1, hour=9, minute=0),
    id="budget_rate_annual_email",
    replace_existing=True
)

if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    scheduler.start()
    logger.info("✅ Scheduler démarré une seule fois")
    logger.info("📧 Email Budget Rate programmé chaque 1er Novembre à 09h00")

atexit.register(lambda: scheduler.shutdown())

# ==============================
# ROUTES DE TEST (à supprimer en production)
# ==============================
@app.route('/test-budget-email')
def test_budget_email():
    """ Route de test pour envoyer manuellement l'email
    URL: http://localhost:5000/test-budget-email?year=2027&email=ton@email.com
    """
    year = request.args.get('year', type=int, default=datetime.now().year + 1)
    email = request.args.get('email', default=BUDGET_OWNER_EMAIL)
    
    success = send_budget_rate_email(
        year=year,
        recipient_email=email,
        test_mode=True  # ✅ Mode test: affiche l'URL au lieu d'envoyer
    )
    
    if success:
        # Récupérer le dernier token créé
        latest_token = list(active_tokens.keys())[-1]
        form_url = f"http://localhost:5000/budget-form/{latest_token}"
        
        return f"""
        <html>
            <body style="font-family: Arial; padding: 50px; background: #f5f7fa;">
                <div style="background: white; padding: 40px; border-radius: 15px; max-width: 800px; margin: 0 auto; box-shadow: 0 4px 20px rgba(0,0,0,0.1);">
                    <h1 style="color: #27ae60;">✅ Email de test préparé</h1>
                    <p><strong>Année :</strong> {year}</p>
                    <p><strong>Destinataire :</strong> {email}</p>
                    <p><strong>Token :</strong> <code>{latest_token}</code></p>
                    <p style="margin-top: 30px;">
                        <a href="{form_url}" style="display: inline-block; background: #0066b2; color: white; padding: 15px 30px; text-decoration: none; border-radius: 8px; font-weight: 600;">
                            🔗 Ouvrir le formulaire de test
                        </a>
                    </p>
                    <p style="margin-top: 20px; color: #7f8c8d; font-size: 14px;">
                        En mode TEST, l'email n'est PAS envoyé. Utilisez le lien ci-dessus pour tester le formulaire.
                    </p>
                </div>
            </body>
        </html>
        """
    else:
        return """
        <html>
            <body style="font-family: Arial; text-align: center; padding: 50px;">
                <h1 style="color: #e74c3c;">❌ Erreur</h1>
                <p>Impossible de préparer l'email de test. Consultez les logs.</p>
            </body>
        </html>
        """, 500

@app.route('/test-insert-budget')
def test_insert_budget():
    """
    Route de test pour insérer des valeurs de test dans la base
    URL: http://localhost:5000/test-insert-budget?year=2027
    """
    year = request.args.get('year', type=int, default=datetime.now().year + 1)
    
    # Valeurs de test
    test_rates = {
        'USD': 1.1000,
        'CNY': 7.9000,
        'GBP': 0.8500,
        'JPY': 155.0000,
        'INR': 90.0000,
        'CAD': 1.4500,
        'AUD': 1.6000,
        'CHF': 0.9500
    }
    
    conn = get_db_connection()
    if not conn:
        return "❌ Erreur connexion base de données", 500
    
    try:
        with conn.cursor() as cur:
            # Supprimer les taux existants
            cur.execute("DELETE FROM fx_budget_rates WHERE year = %s", (year,))
            
            # Insérer les taux de test
            for currency, rate in test_rates.items():
                cur.execute("""
                    INSERT INTO fx_budget_rates (year, currency, budget_rate, updated_at)
                    VALUES (%s, %s, %s, NOW())
                """, (year, currency, rate))
            
            conn.commit()
            
            return f"""
            <html>
                <body style="font-family: Arial; padding: 50px; background: #f5f7fa;">
                    <div style="background: white; padding: 40px; border-radius: 15px; max-width: 600px; margin: 0 auto; box-shadow: 0 4px 20px rgba(0,0,0,0.1);">
                        <h1 style="color: #27ae60;">✅ Insertion réussie</h1>
                        <p><strong>Année :</strong> {year}</p>
                        <p><strong>Devises :</strong> {len(test_rates)}</p>
                        <div style="margin-top: 20px; background: #f8f9fa; padding: 20px; border-radius: 8px;">
                            <h3>Taux insérés :</h3>
                            <ul style="list-style: none; padding: 0;">
                                {''.join([f'<li><strong>{curr}:</strong> {rate}</li>' for curr, rate in test_rates.items()])}
                            </ul>
                        </div>
                    </div>
                </body>
            </html>
            """
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Erreur test insertion: {e}")
        return f"❌ Erreur: {str(e)}", 500
        
    finally:
        conn.close()
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

def get_price_history(days=None, metal_type=None, start_date=None, end_date=None, month=None):
    """
    Récupère l'historique des prix avec filtres.
    Si month est fourni avec start_date/end_date, les dates ont la priorité.
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

            # NOUVELLE LOGIQUE: Gestion intelligente month + dates
            if month and not start_date and not end_date:
                # Cas 1: Seulement month fourni → utiliser tout le mois
                sd, ed = month_to_range(month)
                if sd and ed:
                    query += " AND price_date >= %s AND price_date <= %s"
                    params.extend([sd, ed])
            elif start_date or end_date:
                # Cas 2: Dates spécifiques fournies (avec ou sans month)
                # Les dates ont la priorité
                if start_date:
                    try:
                        sd = datetime.strptime(start_date, '%Y-%m-%d').date()
                        query += " AND price_date >= %s"
                        params.append(sd)
                    except ValueError:
                        logger.warning(f"Format de date invalide pour start_date: {start_date}")
                
                if end_date:
                    try:
                        ed = datetime.strptime(end_date, '%Y-%m-%d').date()
                        query += " AND price_date <= %s"
                        params.append(ed)
                    except ValueError:
                        logger.warning(f"Format de date invalide pour end_date: {end_date}")
            elif days:
                # Cas 3: Filtre par nombre de jours (fallback)
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
        import traceback
        logger.error(traceback.format_exc())
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
    Rapport Florent :
    - Closing Rate du mois sélectionné (M)
    - Period Rate du mois précédent (M-1)
        * Janvier → Décembre de l'année précédente
    - Average YTD : moyenne Janvier → Mois M
    - Budget Rate : taux budgétaire fixe (fx_budget_rates)
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
                    ) AS month_closing
                FROM ecb_exchange_rates
                WHERE EXTRACT(YEAR FROM ref_date) = %s
                   OR (EXTRACT(YEAR FROM ref_date) = %s - 1 AND %s = 1)
            ),

            MonthlyPeriodRates AS (
                -- Period Rate mensuel = moyenne des taux du mois
                SELECT
                    quote_currency,
                    EXTRACT(MONTH FROM ref_date) AS m,
                    AVG(rate) AS period_rate
                FROM MonthlyData
                GROUP BY quote_currency, EXTRACT(MONTH FROM ref_date)
            )

            SELECT 
                md.quote_currency,

                -- ✅ Closing Rate (M)
                MAX(
                    CASE 
                        WHEN EXTRACT(MONTH FROM md.ref_date) = %s
                        THEN md.month_closing
                    END
                ) AS closing_rate,

                -- ✅ Period Rate (M-1 corrigé)
                MAX(
                    CASE 
                        -- Cas Janvier → Décembre année précédente
                        WHEN %s = 1 
                             AND EXTRACT(MONTH FROM md.ref_date) = 12
                        THEN md.month_closing

                        -- Cas normal → mois précédent
                        WHEN %s > 1 
                             AND EXTRACT(MONTH FROM md.ref_date) = %s - 1
                        THEN md.month_closing
                    END
                ) AS period_rate,

                -- ✅ Average YTD
                (
                    SELECT AVG(mpr.period_rate)
                    FROM MonthlyPeriodRates mpr
                    WHERE mpr.quote_currency = md.quote_currency
                      AND mpr.m <= %s
                ) AS ytd_average,

                -- ✅ Budget Rate
                br.budget_rate

            FROM MonthlyData md

            -- ✅ JOIN Budget Table
            LEFT JOIN fx_budget_rates br
                ON md.quote_currency = br.currency
                AND br.year = %s

            GROUP BY md.quote_currency, br.budget_rate
            ORDER BY md.quote_currency;
            """

            # ✅ Paramètres corrigés
            cur.execute(query, (
                year,        # MonthlyData current year
                year,        # MonthlyData previous year condition
                month,       # MonthlyData previous year only if January

                month,       # Closing Rate M

                month,       # Period Rate case January check
                month,       # Period Rate case normal check (>1)
                month,       # Period Rate case normal check (%s - 1)

                month,       # YTD Average

                year         # Budget join
            ))

            return cur.fetchall()

    except Exception as e:
        logger.error(f"Erreur get_florent_report_data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []

    finally:
        conn.close()



def get_monthly_fx_summary(year=None, month=None, quote_currency=None):

    conn = get_db_connection()
    if not conn:
        return []

    try:
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
                ytd.ytd_average,
                br.budget_rate
            FROM MonthlyClosing mc
            LEFT JOIN PreviousMonthClosing pmc 
                ON mc.quote_currency = pmc.quote_currency
            LEFT JOIN YTDAverage ytd 
                ON mc.quote_currency = ytd.quote_currency

            -- ✅ JOIN Budget Rate
            LEFT JOIN fx_budget_rates br
                ON mc.quote_currency = br.currency
                AND br.year = %s

            WHERE 1=1
            """

            params = [
                year, month,
                year, month, year, month,
                year, month,
                year
            ]

            if quote_currency and quote_currency.lower() != 'all':
                query += " AND mc.quote_currency = %s"
                params.append(quote_currency.upper())

            query += " ORDER BY mc.quote_currency"

            cur.execute(query, params)
            return cur.fetchall()

    except Exception as e:
        logger.error(f"Erreur get_monthly_fx_summary: {e}")
        return []

    finally:
        conn.close()

# Route for the monthly summary API
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

        # Serialize decimal values (incluant Budget Rate)
        for key in ['closing_rate', 'period_rate', 'ytd_average', 'budget_rate']:
            if d.get(key) is not None:
                d[key] = float(d[key])

        return d

    data = [serialize_summary(r) for r in summary]

    # Add metadata about the period
    metadata = {
        'year': year or datetime.now().year,
        'month': month or datetime.now().month,
        'month_name': datetime(
            year or datetime.now().year,
            month or datetime.now().month,
            1
        ).strftime('%B %Y'),
        'is_current_month': (
            (year or datetime.now().year) == datetime.now().year
            and (month or datetime.now().month) == datetime.now().month
        )
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
def landing_page():
    return render_template('landing.html')
@app.route('/dashboard')
def dashboard():
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
            month_start = sd
            month_end = ed

        # If user selected start_date -> take the later one
        if start_date:
            try:
                user_sd = datetime.strptime(start_date, "%Y-%m-%d").date()
                month_start = max(month_start, user_sd)
            except ValueError:
                pass

        # If user selected end_date -> take the earlier one
        if end_date:
            try:
                user_ed = datetime.strptime(end_date, "%Y-%m-%d").date()
                month_end = min(month_end, user_ed)
            except ValueError:
                pass

        start_date = month_start.isoformat()
        end_date = month_end.isoformat()
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
    Colonnes:
    Currency | Closing Rate | Period Rate | Average YTD | Budget Rate
    """
    try:
        month = request.args.get('month', type=int)
        year = request.args.get('year', type=int)

        if not month or not year:
            return jsonify({
                'status': 'error',
                'message': 'Paramètres month et year requis'
            }), 400

        month_name = datetime(year, month, 1).strftime('%B %Y')

        # ✅ Données incluant Budget Rate
        data = get_florent_report_data(year, month)

        if not data:
            return jsonify({
                'status': 'error',
                'message': 'Aucune donnée disponible pour cette période'
            }), 404

        wb = Workbook()
        ws = wb.active
        ws.title = "Monthly FX Report"

        # --- STYLING CONFIGURATION ---
        header_fill = PatternFill(start_color="002060", end_color="002060", fill_type="solid")
        header_font = Font(color="FFFFFF", bold=True, size=12)

        border_style = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )

        center_align = Alignment(horizontal='center', vertical='center')
        right_align = Alignment(horizontal='right', vertical='center')

        # =====================================================
        # 1) HEADERS (Budget Rate ajouté)
        # =====================================================
        headers = [
            'Currency',
            f'Closing Rate ({month_name})',
            'Period Rate',
            f'Average YTD ({month_name})',
            'Budget Rate'   # ✅ Nouvelle colonne
        ]

        ws.append(headers)

        # Apply Header Styles
        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = center_align
            cell.border = border_style

        # =====================================================
        # 2) DATA ROWS (Budget Rate ajouté)
        # =====================================================
        for row_data in data:
            row = [
                row_data['quote_currency'],
                row_data['closing_rate'] if row_data['closing_rate'] is not None else None,
                row_data['period_rate'] if row_data['period_rate'] is not None else "N/A",
                row_data['ytd_average'] if row_data['ytd_average'] is not None else None,
                row_data['budget_rate'] if row_data['budget_rate'] is not None else None  # ✅ Budget
            ]
            ws.append(row)

        # =====================================================
        # 3) FORMAT CELLS
        # =====================================================
        for row in ws.iter_rows(min_row=2):
            for idx, cell in enumerate(row):
                cell.border = border_style

                if idx == 0:  # Currency column
                    cell.alignment = center_align

                else:  # Numeric columns
                    if isinstance(cell.value, (float, int)):
                        cell.number_format = '0.0000'
                        cell.alignment = right_align
                    elif cell.value == "N/A":
                        cell.alignment = center_align

        # =====================================================
        # 4) COLUMN WIDTHS (Budget Rate colonne E)
        # =====================================================
        ws.column_dimensions['A'].width = 15
        ws.column_dimensions['B'].width = 22
        ws.column_dimensions['C'].width = 22
        ws.column_dimensions['D'].width = 22
        ws.column_dimensions['E'].width = 18   # ✅ Budget Rate

        # =====================================================
        # 5) EXPORT FILE
        # =====================================================
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
