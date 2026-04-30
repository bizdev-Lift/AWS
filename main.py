from fastapi import FastAPI, Request, Form, HTTPException, BackgroundTasks, Depends, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import xmlrpc.client
import logging
from datetime import datetime, timedelta
import secrets
import hashlib
import asyncio
import json
import os
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from enum import Enum
import requests
from contextlib import asynccontextmanager
import mysql.connector
from mysql.connector import Error
from lift_commerce_integration import LiftCommerceAPI, ShipmentResult
from lift_commerce_integration_v2 import LiftCommerceWebShipAPI, OrderResult

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Background task manager
background_tasks_active = True

# Database configuration - ALL values must be set via environment variables
DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')
DB_USERNAME = os.environ.get('DB_USERNAME')
DB_PASSWORD = os.environ.get('DB_PASSWORD')

if not all([DB_HOST, DB_NAME, DB_USERNAME, DB_PASSWORD]):
    logger.error("CRITICAL: Database credentials not configured. Set DB_HOST, DB_NAME, DB_USERNAME, DB_PASSWORD environment variables")
    DB_HOST = DB_NAME = DB_USERNAME = DB_PASSWORD = None

class DatabaseManager:
    """Manages persistent storage of order processing states"""
    
    def __init__(self):
        self.connection = None
        
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USERNAME,
                password=DB_PASSWORD,
                autocommit=True
            )
            
            if self.connection.is_connected():
                logger.info(f"Connected to MySQL database: {DB_NAME}")
                self.initialize_tables()
                return True
                
        except Error as e:
            logger.error(f"Database connection error: {e}")
            return False
    
    def initialize_tables(self):
        """Create necessary tables if they don't exist"""
        try:
            cursor = self.connection.cursor()
            
            create_orders_table = """
            CREATE TABLE IF NOT EXISTS processed_orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                order_id INT UNIQUE NOT NULL,
                order_name VARCHAR(50) NOT NULL,
                partner_name VARCHAR(255),
                total_amount DECIMAL(10,2),
                status ENUM('pending', 'processing', 'synced', 'shipped', 'delivered', 'failed') DEFAULT 'pending',
                lift_commerce_id VARCHAR(100),
                retry_count INT DEFAULT 0,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                last_sync_attempt TIMESTAMP NULL,
                order_data JSON,
                book_number VARCHAR(50) NULL,
                tracking_number VARCHAR(100) NULL,
                carrier_code VARCHAR(20) NULL,
                service_code VARCHAR(50) NULL,
                label_url VARCHAR(500) NULL,
                shipped_at TIMESTAMP NULL,
                INDEX idx_order_id (order_id),
                INDEX idx_status (status),
                INDEX idx_created_at (created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            
            cursor.execute(create_orders_table)
            logger.info("Database tables initialized successfully")
            cursor.close()
            
        except Error as e:
            logger.error(f"Error creating tables: {e}")
    
    def is_order_processed(self, order_id: int) -> bool:
        """Check if order has already been processed"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT status FROM processed_orders WHERE order_id = %s", (order_id,))
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                status = result[0]
                return status in ['synced', 'shipped', 'delivered']
            return False
            
        except Error as e:
            logger.error(f"Error checking order status: {e}")
            return False
    
    def save_order(self, awds_order) -> bool:
        """Save or update order in database"""
        try:
            cursor = self.connection.cursor()
            
            order_data_json = json.dumps({
                'products': [asdict(p) for p in awds_order.products],
                'shipping_address': asdict(awds_order.shipping_address),
                'date_order': awds_order.date_order.isoformat()
            }, default=str)
            
            upsert_query = """
            INSERT INTO processed_orders 
            (order_id, order_name, partner_name, total_amount, status, 
             lift_commerce_id, retry_count, error_message, last_sync_attempt, order_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                status = VALUES(status),
                lift_commerce_id = VALUES(lift_commerce_id),
                retry_count = VALUES(retry_count),
                error_message = VALUES(error_message),
                last_sync_attempt = VALUES(last_sync_attempt),
                updated_at = CURRENT_TIMESTAMP
            """
            
            error_msg = awds_order.error_messages[-1] if awds_order.error_messages else None
            last_attempt = awds_order.last_sync_attempt.strftime('%Y-%m-%d %H:%M:%S') if awds_order.last_sync_attempt else None
            
            cursor.execute(upsert_query, (
                awds_order.id,
                awds_order.name,
                awds_order.partner_name,
                awds_order.total_amount,
                awds_order.status.value,
                awds_order.lift_commerce_id,
                awds_order.retry_count,
                error_msg,
                last_attempt,
                order_data_json
            ))
            
            cursor.close()
            return True
            
        except Error as e:
            logger.error(f"Error saving order {awds_order.name}: {e}")
            return False

    def update_order_tracking(
        self,
        order_id: int,
        book_number: str,
        tracking_number: str,
        carrier_code: str,
        service_code: str,
        label_url: Optional[str] = None
    ) -> bool:
        """Update order with Lift Commerce tracking information"""
        try:
            cursor = self.connection.cursor()

            update_query = """
            UPDATE processed_orders
            SET book_number = %s,
                tracking_number = %s,
                carrier_code = %s,
                service_code = %s,
                label_url = %s,
                shipped_at = NOW(),
                status = 'shipped',
                updated_at = NOW()
            WHERE order_id = %s
            """

            cursor.execute(update_query, (
                book_number,
                tracking_number,
                carrier_code,
                service_code,
                label_url,
                order_id
            ))

            cursor.close()
            logger.info(f"Updated tracking for order {order_id}: Book #{book_number}, Tracking #{tracking_number}")
            return True

        except Error as e:
            logger.error(f"Error updating tracking for order {order_id}: {e}")
            return False

    def get_metrics(self) -> Dict:
        """Get processing metrics from database"""
        try:
            cursor = self.connection.cursor()
            
            cursor.execute("""
                SELECT status, COUNT(*) as count 
                FROM processed_orders 
                GROUP BY status
            """)
            status_counts = dict(cursor.fetchall())
            
            cursor.execute("""
                SELECT COUNT(*) as today_count
                FROM processed_orders 
                WHERE DATE(created_at) = CURDATE()
            """)
            today_count = cursor.fetchone()[0]
            
            total = sum(status_counts.values())
            successful = status_counts.get('synced', 0) + status_counts.get('shipped', 0) + status_counts.get('delivered', 0)
            success_rate = (successful / total * 100) if total > 0 else 0
            
            cursor.close()
            
            return {
                'total_orders_processed': total,
                'successful_syncs': successful,
                'failed_syncs': status_counts.get('failed', 0),
                'pending_syncs': status_counts.get('pending', 0) + status_counts.get('processing', 0),
                'success_rate': success_rate,
                'orders_today': today_count,
                'system_status': 'running'
            }
            
        except Error as e:
            logger.error(f"Error getting metrics: {e}")
            return {'total_orders_processed': 0, 'successful_syncs': 0, 'failed_syncs': 0, 'pending_syncs': 0, 'success_rate': 0, 'orders_today': 0, 'system_status': 'database_error'}
    
    def get_recent_orders(self, limit: int = 10) -> List[Dict]:
        """Get recent orders for dashboard display"""
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute("""
                SELECT order_id, order_name, partner_name, total_amount, 
                       status, lift_commerce_id, created_at, updated_at
                FROM processed_orders 
                ORDER BY updated_at DESC
                LIMIT %s
            """, (limit,))
            
            results = cursor.fetchall()
            cursor.close()
            return results
            
        except Error as e:
            logger.error(f"Error getting recent orders: {e}")
            return []

# Initialize database manager
db_manager = DatabaseManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Connect to database
    logger.info("Connecting to database...")
    if not db_manager.connect():
        logger.error("Failed to connect to database - running in memory mode")
    
    # Start background tasks after a delay
    asyncio.create_task(delayed_background_startup())
    yield
    
    # Stop background tasks
    global background_tasks_active
    background_tasks_active = False

async def delayed_background_startup():
    """Start background tasks after web server is ready"""
    await asyncio.sleep(10)  # Wait 10 seconds for web server to be ready
    logger.info("Starting background tasks...")
    asyncio.create_task(order_monitoring_task())
    asyncio.create_task(tracking_update_task())

app = FastAPI(title="AWDS Lift Commerce Connector - Production", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Authentication - Password must be set via environment variable
security = HTTPBasic()
USERNAME = os.environ.get('ADMIN_USERNAME', 'admin')
PASSWORD_HASH = os.environ.get('ADMIN_PASSWORD_HASH')

if not PASSWORD_HASH:
    logger.error("CRITICAL: ADMIN_PASSWORD_HASH not configured. Dashboard authentication will fail.")
    PASSWORD_HASH = ""

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    current_username_bytes = credentials.username.encode("utf8")
    correct_username_bytes = USERNAME.encode("utf8")
    is_correct_username = secrets.compare_digest(current_username_bytes, correct_username_bytes)
    
    current_password_hash = hashlib.sha256(credentials.password.encode("utf8")).hexdigest()
    is_correct_password = secrets.compare_digest(current_password_hash, PASSWORD_HASH)
    
    if not (is_correct_username and is_correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

# Configuration - AWDS Production Settings - ALL must be set via environment variables
ODOO_URL = os.environ.get('ODOO_URL')
ODOO_DB = os.environ.get('ODOO_DB')
ODOO_USERNAME = os.environ.get('ODOO_USERNAME')
ODOO_API_KEY = os.environ.get('ODOO_API_KEY')

if not all([ODOO_URL, ODOO_DB, ODOO_USERNAME, ODOO_API_KEY]):
    logger.error("CRITICAL: Odoo credentials not configured. Set ODOO_URL, ODOO_DB, ODOO_USERNAME, ODOO_API_KEY environment variables")
    ODOO_URL = ODOO_DB = ODOO_USERNAME = ODOO_API_KEY = None

# Lift Commerce API Configuration - Must be set via environment variables
LIFT_API_KEY = os.environ.get('LIFT_API_KEY')
LIFT_CUSTOMER_ID = os.environ.get('LIFT_CUSTOMER_ID')
LIFT_INTEGRATION_ID = os.environ.get('LIFT_INTEGRATION_ID')
LIFT_API_BASE_URL = os.environ.get('LIFT_API_BASE_URL', 'https://login.liftcommerce.io/restapi/v1')
WEBHOOK_SECRET = os.environ.get('WEBHOOK_SECRET')

if not all([LIFT_API_KEY, LIFT_CUSTOMER_ID]):
    logger.error("CRITICAL: Lift Commerce credentials not configured. Set LIFT_API_KEY, LIFT_CUSTOMER_ID environment variables")
    LIFT_API_KEY = LIFT_CUSTOMER_ID = None

if not WEBHOOK_SECRET:
    logger.warning("WEBHOOK_SECRET not set. Generating random secret (will change on restart!)")
    WEBHOOK_SECRET = secrets.token_hex(32)

# Testing Mode
DRY_RUN = os.environ.get('DRY_RUN', 'false').lower() in ('true', '1', 'yes')
if DRY_RUN:
    logger.warning("🧪 DRY RUN MODE ENABLED - No real shipping labels will be created!")
    logger.warning("   Set DRY_RUN=false in .env to enable production mode")
else:
    logger.info("✅ Production mode - Real shipping labels will be created")

# Odoo write-back safety gate — OFF by default
# Only enable after manually testing with /test/odoo-tracking endpoint
ODOO_WRITEBACK_ENABLED = os.environ.get('ODOO_WRITEBACK_ENABLED', 'false').lower() in ('true', '1', 'yes')
if ODOO_WRITEBACK_ENABLED:
    logger.info("Odoo tracking write-back: ENABLED")
else:
    logger.warning("Odoo tracking write-back: DISABLED (dry run) — set ODOO_WRITEBACK_ENABLED=true to enable")

# Data Models
class OrderStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing" 
    SYNCED = "synced"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    FAILED = "failed"

@dataclass
class ProductData:
    sku: str
    name: str
    weight: float
    quantity: int
    price: float
    
    def get_weight(self) -> float:
        return self.weight if self.weight > 0 else 1.0

@dataclass
class ShippingAddress:
    name: str
    street1: str
    street2: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    country: str = "US"
    phone: str = ""
    
    def validate(self) -> List[str]:
        errors = []
        if not self.name: errors.append("Missing recipient name")
        if not self.street1: errors.append("Missing street address")
        if not self.city: errors.append("Missing city")
        if not self.state: errors.append("Missing state")
        if not self.zip_code: errors.append("Missing zip code")
        return errors

@dataclass
class AWDSOrder:
    id: int
    name: str
    partner_id: int
    partner_name: str
    total_amount: float
    date_order: datetime
    shipping_address: ShippingAddress
    products: List[ProductData]
    status: OrderStatus = OrderStatus.PENDING
    lift_commerce_id: Optional[str] = None
    tracking_numbers: List[str] = None
    error_messages: List[str] = None
    retry_count: int = 0
    last_sync_attempt: Optional[datetime] = None
    
    def __post_init__(self):
        if self.tracking_numbers is None:
            self.tracking_numbers = []
        if self.error_messages is None:
            self.error_messages = []

# Storage with database integration
config_store = {
    "api_url": "https://api.liftcommerce.com/v1",
    "api_key": "",
    "warehouse_id": "",
    "default_weight": 1.0,
    "batch_size": 50,
    "retry_attempts": 3,
    "monitoring_enabled": True
}

# Fallback in-memory stores (used when database is unavailable)
processed_orders: Dict[int, AWDSOrder] = {}
processing_queue: List[int] = []
failed_orders: Dict[int, AWDSOrder] = {}

class OdooConnector:
    """Production Odoo connector with error handling and optimization"""
    
    def __init__(self):
        self.common = None
        self.models = None
        self.uid = None
        self._last_auth = None
    
    async def authenticate(self):
        try:
            if self._last_auth and (datetime.now() - self._last_auth).seconds < 3600:
                return self.uid
            
            self.common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
            self.uid = self.common.authenticate(ODOO_DB, ODOO_USERNAME, ODOO_API_KEY, {})
            
            if not self.uid:
                raise Exception("Odoo authentication failed")
            
            self.models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object')
            self._last_auth = datetime.now()
            logger.info(f"Odoo authentication successful, UID: {self.uid}")
            return self.uid
            
        except Exception as e:
            logger.error(f"Odoo authentication error: {e}")
            raise

    async def get_ready_orders(self, limit: int = 100) -> List[AWDSOrder]:
        """Get orders ready for fulfillment with comprehensive data"""
        try:
            await self.authenticate()
            
            # Search for orders in 'sale' state (Ready to Fulfill)
            order_ids = self.models.execute_kw(
                ODOO_DB, self.uid, ODOO_API_KEY,
                'sale.order',
                'search',
                [[('state', '=', 'sale')]],
                {'limit': limit, 'order': 'id desc'}
            )
            
            if not order_ids:
                return []
            
            # Filter out already processed orders using database
            if db_manager.connection:
                unprocessed_order_ids = []
                for order_id in order_ids:
                    if not db_manager.is_order_processed(order_id):
                        unprocessed_order_ids.append(order_id)
                order_ids = unprocessed_order_ids
            
            if not order_ids:
                logger.info("No new orders to process")
                return []
            
            # Get detailed order information
            orders_data = self.models.execute_kw(
                ODOO_DB, self.uid, ODOO_API_KEY,
                'sale.order',
                'read',
                [order_ids],
                {'fields': ['name', 'partner_id', 'partner_shipping_id', 'order_line', 
                          'date_order', 'amount_total', 'state']}
            )
            
            awds_orders = []
            EXCLUDED_PARTNERS = ["staples", "essendant"]
            for order_data in orders_data:
                try:
                    # Filter out excluded partners EARLY (before expensive lookups)
                    partner_name = order_data['partner_id'][1] if order_data.get('partner_id') else ''
                    if any(excluded in partner_name.lower() for excluded in EXCLUDED_PARTNERS):
                        logger.info(f"Skipping order {order_data['name']} - excluded partner: {partner_name}")
                        continue

                    # Get shipping address
                    partner_id = order_data['partner_shipping_id'][0] if order_data['partner_shipping_id'] else order_data['partner_id'][0]
                    address_data = self.models.execute_kw(
                        ODOO_DB, self.uid, ODOO_API_KEY,
                        'res.partner',
                        'read',
                        [partner_id],
                        {'fields': ['name', 'street', 'street2', 'city', 'state_id', 'zip', 'country_id', 'phone']}
                    )[0]

                    # Lift Commerce API requires 2-letter ISO codes for state/country.
                    # Odoo's *_id[1] returns the display name (e.g. "California (US)",
                    # "United States") which fails Lift's `^[A-Z][A-Z]$` validator with
                    # 422. Resolve `.code` from res.country / res.country.state instead.
                    state_code = ''
                    if address_data.get('state_id'):
                        state_rec = self.models.execute_kw(
                            ODOO_DB, self.uid, ODOO_API_KEY,
                            'res.country.state', 'read',
                            [[address_data['state_id'][0]]],
                            {'fields': ['code']}
                        )
                        if state_rec:
                            state_code = state_rec[0].get('code', '') or ''
                    country_code = 'US'
                    if address_data.get('country_id'):
                        country_rec = self.models.execute_kw(
                            ODOO_DB, self.uid, ODOO_API_KEY,
                            'res.country', 'read',
                            [[address_data['country_id'][0]]],
                            {'fields': ['code']}
                        )
                        if country_rec:
                            country_code = (country_rec[0].get('code') or 'US')

                    shipping_address = ShippingAddress(
                        name=address_data.get('name', '') or '',
                        street1=address_data.get('street', '') or '',
                        street2=address_data.get('street2', '') or '',
                        city=address_data.get('city', '') or '',
                        state=state_code,
                        zip_code=address_data.get('zip', '') or '',
                        country=country_code,
                        phone=address_data.get('phone', '') or ''
                    )
                    
                    # Get order line products
                    products = []
                    if order_data['order_line']:
                        lines_data = self.models.execute_kw(
                            ODOO_DB, self.uid, ODOO_API_KEY,
                            'sale.order.line',
                            'read',
                            [order_data['order_line']],
                            {'fields': ['product_id', 'product_uom_qty', 'price_unit']}
                        )
                        
                        for line in lines_data:
                            if line['product_id']:
                                product_data = self.models.execute_kw(
                                    ODOO_DB, self.uid, ODOO_API_KEY,
                                    'product.product',
                                    'read',
                                    [line['product_id'][0]],
                                    {'fields': ['default_code', 'name', 'weight']}
                                )[0]
                                
                                product = ProductData(
                                    sku=product_data.get('default_code', f"SKU-{line['product_id'][0]}"),
                                    name=product_data.get('name', 'Unknown Product'),
                                    weight=product_data.get('weight', 0),
                                    quantity=int(line['product_uom_qty']),
                                    price=line['price_unit']
                                )
                                products.append(product)
                    
                    awds_order = AWDSOrder(
                        id=order_data['id'],
                        name=order_data['name'],
                        partner_id=order_data['partner_id'][0],
                        partner_name=order_data['partner_id'][1],
                        total_amount=order_data['amount_total'],
                        date_order=datetime.fromisoformat(order_data['date_order'].replace(' ', 'T')),
                        shipping_address=shipping_address,
                        products=products
                    )
                    
                    awds_orders.append(awds_order)
                    logger.info(f"Processed order {awds_order.name} with {len(products)} products")
                    
                except Exception as e:
                    logger.error(f"Error processing order {order_data.get('name', 'unknown')}: {e}")
                    continue
            
            return awds_orders
            
        except Exception as e:
            logger.error(f"Error fetching ready orders: {e}")
            return []

    async def post_failure_note(self, order_id: int, order_name: str, error: str) -> bool:
        """
        Post an internal chatter note on a sale.order when the connector
        couldn't push it to WebShip (validation failure, API error, etc.).
        Uses mail.mt_note subtype so AWDUS staff see it on the order's
        chatter without sending an email to the customer.
        """
        try:
            await self.authenticate()
            note = (
                f"⚠️ Lift Commerce Connector could not push this order to WebShip.\n\n"
                f"Reason: {error}\n\n"
                f"Please correct the order in Odoo (e.g. shipping address, "
                f"recipient name) — the connector will retry automatically on "
                f"the next sync cycle (every 5 minutes)."
            )
            self.models.execute_kw(
                ODOO_DB, self.uid, ODOO_API_KEY,
                'sale.order', 'message_post',
                [[order_id]],
                {
                    'body': note,
                    'message_type': 'comment',
                    'subtype_xmlid': 'mail.mt_note',
                },
            )
            logger.info(f"Odoo: posted failure note on sale.order {order_name} (id {order_id})")
            return True
        except Exception as e:
            logger.warning(f"Odoo: failed to post failure note for {order_name} (non-fatal): {e}")
            return False

    async def update_tracking_in_odoo(
        self,
        order_name: str,
        tracking_number: str,
        carrier: str = "FedEx",
        secondary_tracking: Optional[str] = None,
        service_code: str = "",
    ) -> bool:
        """
        Write tracking number back to Odoo delivery order (stock.picking).
        Only updates carrier_tracking_ref — does NOT change order state to avoid
        triggering unwanted automations (email notifications, etc.).

        For multi-leg shipments (e.g. FedEx SmartPost) where Lift sends both a
        carrier parent number and a sub-leg number, both are written to
        carrier_tracking_ref joined by " / " so AWDUS staff can match WebShip's
        display to Odoo without confusion. When the service is SmartPost, an
        internal chatter note is also posted on the sale order explaining the
        dual-tracking relationship (mt_note subtype — no email goes out).
        """
        try:
            await self.authenticate()

            # Find the sale order by name
            sale_order_ids = self.models.execute_kw(
                ODOO_DB, self.uid, ODOO_API_KEY,
                'sale.order', 'search',
                [[('name', '=', order_name)]],
                {'limit': 1}
            )

            if not sale_order_ids:
                logger.warning(f"Odoo: Sale order {order_name} not found")
                return False

            # Get the delivery pickings linked to this sale order
            sale_order_data = self.models.execute_kw(
                ODOO_DB, self.uid, ODOO_API_KEY,
                'sale.order', 'read',
                [sale_order_ids],
                {'fields': ['picking_ids']}
            )

            sale_order = sale_order_data[0] if sale_order_data else {}
            picking_ids = sale_order.get('picking_ids', [])
            if not picking_ids:
                logger.warning(f"Odoo: No delivery orders found for {order_name}")
                return False

            # Find the outgoing picking (delivery, not receipt)
            pickings = self.models.execute_kw(
                ODOO_DB, self.uid, ODOO_API_KEY,
                'stock.picking', 'read',
                [picking_ids],
                {'fields': ['picking_type_code', 'state', 'carrier_tracking_ref']}
            )

            target_picking = None
            for picking in pickings:
                if picking.get('picking_type_code') == 'outgoing':
                    target_picking = picking
                    break

            # Fallback: use last picking if no outgoing found
            if not target_picking and pickings:
                target_picking = pickings[-1]

            if not target_picking:
                logger.warning(f"Odoo: No suitable picking found for {order_name}")
                return False

            # Build the tracking value — concatenate primary + secondary when both
            # are present and distinct. SmartPost dual-tracking surfaces here:
            # e.g. "61290386139620652543 / 398579708303".
            if secondary_tracking and secondary_tracking != tracking_number:
                tracking_value = f"{tracking_number} / {secondary_tracking}"
            else:
                tracking_value = tracking_number

            # Only update carrier_tracking_ref — nothing else
            self.models.execute_kw(
                ODOO_DB, self.uid, ODOO_API_KEY,
                'stock.picking', 'write',
                [[target_picking['id']], {'carrier_tracking_ref': tracking_value}]
            )

            logger.info(f"Odoo: Updated tracking for {order_name} → {tracking_value} (picking {target_picking['id']})")

            # For SmartPost, post an internal chatter note on the sale order
            # explaining the dual-tracking-number relationship. This heads off the
            # recurring "WebShip and Odoo numbers don't match" question from
            # AWDUS staff. Uses subtype mail.mt_note → internal-only, no email.
            is_smartpost = (
                'smart_post' in service_code.lower()
                or 'smartpost' in service_code.lower()
            )
            if is_smartpost:
                try:
                    # Plain text — Odoo's message_post over XML-RPC escapes raw
                    # HTML in `body`, so we let Odoo wrap our text in <p> itself.
                    # Result renders cleanly as a chatter note.
                    note_lines = [
                        f"📦 Tracking updated: {tracking_number}",
                        "",
                        "This is a FedEx SmartPost shipment. WebShip may "
                        "display a shorter USPS handoff number"
                        + (f" ({secondary_tracking})" if secondary_tracking else "")
                        + " for the last-mile delivery — both numbers track "
                        "the same parcel. Either can be entered at fedex.com.",
                    ]
                    note_body = "\n".join(note_lines)
                    self.models.execute_kw(
                        ODOO_DB, self.uid, ODOO_API_KEY,
                        'sale.order', 'message_post',
                        [sale_order_ids],
                        {
                            'body': note_body,
                            'message_type': 'comment',
                            'subtype_xmlid': 'mail.mt_note',
                        },
                    )
                    logger.info(f"Odoo: Posted SmartPost note on sale.order for {order_name}")
                except Exception as note_err:
                    # Chatter post is non-critical — log and continue
                    logger.warning(
                        f"Odoo: SmartPost note post failed for {order_name} "
                        f"(non-fatal): {note_err}"
                    )

            return True

        except Exception as e:
            logger.error(f"Odoo: Failed to update tracking for {order_name}: {e}")
            return False


class LiftCommerceConnector:
    """
    Pushes orders into the Lift Commerce WebShip queue (PUT order).

    Does NOT auto-create FedEx labels. AWDUS staff finalize shipping in
    WebShip on their own — picking carrier, weight, packaging — and Lift's
    webhook back to /webhook/tracking is what writes the tracking number
    to Odoo.
    """

    def __init__(self):
        self.api = LiftCommerceWebShipAPI(
            api_key=LIFT_API_KEY,
            customer_id=LIFT_CUSTOMER_ID,
            integration_id=LIFT_INTEGRATION_ID,
            dry_run=DRY_RUN
        )
        self.warehouse_address = {
            "name": "Shipping Manager",
            "company": "Fulfillment Center",
            "address1": "123 Warehouse St",
            "address2": "",
            "city": "Salt Lake City",
            "state": "UT",
            "zip": "84106",
            "country": "US",
            "phone": "8015551234",
            "email": "warehouse@awds.com"
        }

    async def queue_order(self, awds_order: AWDSOrder) -> OrderResult:
        """
        Push an order into the WebShip queue. Returns OrderResult with
        success/order_id; tracking arrives later via webhook from Lift.
        """
        try:
            address_errors = awds_order.shipping_address.validate()
            if address_errors:
                raise ValueError(f"Address validation failed: {', '.join(address_errors)}")

            receiver = {
                "name": awds_order.shipping_address.name,
                "company": "",
                "address1": awds_order.shipping_address.street1,
                "address2": awds_order.shipping_address.street2 or "",
                "city": awds_order.shipping_address.city,
                "state": awds_order.shipping_address.state,
                "zip": awds_order.shipping_address.zip_code,
                "country": awds_order.shipping_address.country,
                "phone": awds_order.shipping_address.phone or "",
                "email": "",
            }

            # Build items list from Odoo order lines.
            # Lift's PUT order requires these field names per their 422 errors:
            # sku, productId, title, price, imgUrl, htsNumber, countryOfOrigin,
            # lineId. Empty strings are acceptable for fields we don't have in
            # Odoo (imgUrl, htsNumber).
            items = []
            for idx, p in enumerate(awds_order.products, start=1):
                items.append({
                    "lineId": str(idx),
                    "sku": p.sku,
                    "productId": p.sku,
                    "title": p.name,
                    "name": p.name,
                    "quantity": p.quantity,
                    "price": str(p.price or 0),
                    "unitPrice": str(p.price or 0),
                    "weight": str(p.weight or 0),
                    "imgUrl": "",
                    "htsNumber": "",
                    "countryOfOrigin": "US",
                })

            order_date = awds_order.date_order.strftime("%Y-%m-%d") if awds_order.date_order else datetime.now().strftime("%Y-%m-%d")

            result = self.api.put_order(
                order_id=str(awds_order.id),
                order_number=awds_order.name,
                order_date=order_date,
                sender=self.warehouse_address,
                receiver=receiver,
                items=items,
                shipping_total=0.0,
            )

            if result.success:
                logger.info(f"✅ Order {awds_order.name} pushed to WebShip queue (order_id={result.order_id})")
            else:
                logger.error(f"❌ Failed to push {awds_order.name} to WebShip: {result.error_message}")

            return result

        except Exception as e:
            logger.error(f"Error queueing order {awds_order.name}: {e}")
            return OrderResult(success=False, error_message=str(e))

# Initialize connectors
odoo_connector = OdooConnector()
lc_connector = LiftCommerceConnector()

async def order_monitoring_task():
    """Background task for continuous order monitoring with database persistence"""
    logger.info("Starting order monitoring task")
    
    while background_tasks_active:
        try:
            if not config_store.get("monitoring_enabled"):
                await asyncio.sleep(60)
                continue
            
            logger.info("Running order monitoring cycle")
            
            # Get ready orders from Odoo
            ready_orders = await odoo_connector.get_ready_orders(
                limit=config_store.get("batch_size", 50)
            )
            
            # Process new orders and save to database
            new_orders_count = 0
            for order in ready_orders:
                # Save order to database
                if db_manager.connection:
                    db_manager.save_order(order)
                else:
                    # Fallback to in-memory storage
                    processed_orders[order.id] = order
                    if order.id not in processing_queue:
                        processing_queue.append(order.id)
                
                new_orders_count += 1
            
            if new_orders_count > 0:
                logger.info(f"Added {new_orders_count} new orders to processing")
            
            # Process orders (attempt to sync to Lift Commerce)
            await process_orders()
            
            logger.info(f"Monitoring cycle complete. Processed {new_orders_count} new orders")
            
        except Exception as e:
            logger.error(f"Error in order monitoring task: {e}")
        
        # Wait 5 minutes between monitoring cycles
        await asyncio.sleep(300)

async def process_orders():
    """Process orders with Lift Commerce Book Shipment integration"""
    try:
        # Get orders that need processing from database or memory
        if db_manager.connection:
            cursor = db_manager.connection.cursor(dictionary=True)
            cursor.execute("""
                SELECT order_id, order_name, partner_name, total_amount, order_data
                FROM processed_orders
                WHERE status IN ('pending', 'failed')
                AND retry_count < 3
                LIMIT 10
            """)
            pending_orders = cursor.fetchall()
            cursor.close()
        else:
            # Fallback to in-memory processing
            pending_orders = [{'order_id': oid} for oid in processing_queue[:10]]

        for order_data in pending_orders:
            order_id = order_data['order_id']

            try:
                # Get full order details from Odoo
                orders = await odoo_connector.get_ready_orders(limit=200)
                target_order = None

                for order in orders:
                    if order.id == order_id:
                        target_order = order
                        break

                if not target_order:
                    # Order is in our queue but Odoo no longer returns it (likely
                    # delivered/archived). Without this UPDATE the row would stay
                    # `pending` with retry_count=0 forever and block all newer
                    # orders behind the SELECT LIMIT 10. Increment retry so the
                    # row gets marked `failed` after 3 cycles and falls out of
                    # the queue head.
                    logger.warning(f"Order {order_id} not found in Odoo — marking as stale")
                    if db_manager.connection:
                        stale_cur = db_manager.connection.cursor()
                        stale_cur.execute("""
                            UPDATE processed_orders
                            SET retry_count = retry_count + 1,
                                error_message = 'Order no longer in Odoo ready list',
                                last_sync_attempt = NOW(),
                                status = CASE
                                    WHEN retry_count + 1 >= 3 THEN 'failed'
                                    ELSE 'pending'
                                END
                            WHERE order_id = %s
                        """, (order_id,))
                        stale_cur.close()
                    continue

                # Push order into WebShip queue (NOT auto-create label).
                # AWDUS staff finalize shipping in WebShip; tracking arrives
                # later via webhook back to /webhook/tracking.
                result = await lc_connector.queue_order(target_order)

                if result.success:
                    if db_manager.connection:
                        ok_cur = db_manager.connection.cursor()
                        ok_cur.execute("""
                            UPDATE processed_orders
                            SET status = 'synced',
                                last_sync_attempt = NOW(),
                                error_message = NULL,
                                lift_commerce_id = %s
                            WHERE order_id = %s
                        """, (result.order_id, order_id))
                        ok_cur.close()
                    logger.info(
                        f"✅ Order {order_data['order_name']} pushed to WebShip queue "
                        f"(awaiting manual labeling on Lift side)"
                    )
                else:
                    raise Exception(result.error_message or "Unknown error")

            except Exception as e:
                # Handle failure - increment retry count
                if db_manager.connection:
                    cursor = db_manager.connection.cursor()
                    cursor.execute("""
                        UPDATE processed_orders
                        SET retry_count = retry_count + 1,
                            error_message = %s,
                            last_sync_attempt = NOW(),
                            status = CASE
                                WHEN retry_count + 1 >= 3 THEN 'failed'
                                ELSE 'pending'
                            END
                        WHERE order_id = %s
                    """, (str(e)[:500], order_id))
                    cursor.close()
                    logger.warning(f"❌ Order {order_id} processing failed: {e}")

                # Post a note on the Odoo sale.order so AWDUS staff see why
                # this order isn't flowing through. Best-effort, non-fatal.
                order_name = order_data.get('order_name') if isinstance(order_data, dict) else ''
                try:
                    await odoo_connector.post_failure_note(order_id, order_name, str(e)[:500])
                except Exception as note_err:
                    logger.warning(f"Failure-note post failed for {order_id}: {note_err}")

    except Exception as e:
        logger.error(f"Error in process_orders: {e}")

async def tracking_update_task():
    """Background task for tracking number updates"""
    logger.info("Starting tracking update task")
    
    while background_tasks_active:
        try:
            await asyncio.sleep(1800)  # Check every 30 minutes
            logger.info("Checking for tracking updates...")
            
        except Exception as e:
            logger.error(f"Error in tracking update task: {e}")

# Dashboard Routes
@app.get("/", response_class=HTMLResponse)
async def production_dashboard(request: Request, username: str = Depends(verify_credentials)):
    # Get data from database or fallback to memory
    if db_manager.connection:
        metrics_data = db_manager.get_metrics()
        recent_orders = db_manager.get_recent_orders(10)
        failed_orders_list = []  # Get from database
        queue_size = metrics_data.get('pending_syncs', 0)
    else:
        # Fallback to in-memory storage (use global failed_orders dict)
        global failed_orders
        metrics_data = {'total_orders_processed': len(processed_orders), 'successful_syncs': 0, 'failed_syncs': 0, 'pending_syncs': len(processing_queue), 'success_rate': 0, 'system_status': 'memory_mode'}
        recent_orders = list(processed_orders.values())[-10:]
        failed_orders_list = list(failed_orders.values())[-5:]
        queue_size = len(processing_queue)

    return templates.TemplateResponse("production_dashboard.html", {
        "request": request,
        "config": config_store,
        "metrics": metrics_data,
        "recent_orders": recent_orders,
        "failed_orders": failed_orders_list,
        "queue_size": queue_size,
        "username": username
    })

@app.get("/health")
async def health_check():
    if db_manager.connection:
        metrics_data = db_manager.get_metrics()
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "database": "connected",
            "queue_size": metrics_data.get('pending_syncs', 0),
            "processed_orders": metrics_data.get('total_orders_processed', 0),
            "success_rate": metrics_data.get('success_rate', 0)
        }
    else:
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "database": "disconnected",
            "queue_size": len(processing_queue),
            "processed_orders": len(processed_orders),
            "success_rate": 0
        }

@app.post("/config")
async def update_config(
    api_url: str = Form(...),
    api_key: str = Form(...),
    warehouse_id: str = Form(...),
    default_weight: float = Form(1.0),
    batch_size: int = Form(50),
    monitoring_enabled: bool = Form(True),
    username: str = Depends(verify_credentials)
):
    config_store.update({
        "api_url": api_url,
        "api_key": api_key,
        "warehouse_id": warehouse_id,
        "default_weight": default_weight,
        "batch_size": batch_size,
        "monitoring_enabled": monitoring_enabled
    })
    logger.info(f"Configuration updated by {username}")
    return RedirectResponse(url="/", status_code=302)

@app.get("/orders/ready")
async def get_ready_orders_api(username: str = Depends(verify_credentials)):
    orders = await odoo_connector.get_ready_orders(limit=20)
    return {
        "orders": [asdict(order) for order in orders],
        "count": len(orders)
    }

@app.post("/orders/{order_id}/sync")
async def manual_sync_order(order_id: int, username: str = Depends(verify_credentials)):
    try:
        # Get order from Odoo and add to processing
        orders = await odoo_connector.get_ready_orders(limit=100)
        target_order = None
        
        for order in orders:
            if order.id == order_id:
                target_order = order
                break
        
        if not target_order:
            raise HTTPException(status_code=404, detail="Order not found")
        
        # Save to database for processing
        if db_manager.connection:
            db_manager.save_order(target_order)
        else:
            processed_orders[order_id] = target_order
            if order_id not in processing_queue:
                processing_queue.append(order_id)
        
        logger.info(f"Order {order_id} manually queued by {username}")
        return {"status": "success", "message": f"Order {order_id} queued for sync"}
        
    except Exception as e:
        logger.error(f"Error manually syncing order {order_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/documentation", response_class=HTMLResponse)
async def documentation(request: Request, username: str = Depends(verify_credentials)):
    return templates.TemplateResponse("docs.html", {
        "request": request, "username": username
    })

@app.post("/test/odoo-tracking/{order_name}")
async def test_odoo_tracking_update(
    order_name: str,
    tracking_number: str = "TEST-TRACKING-123",
    dry_run: bool = True,
    username: str = Depends(verify_credentials)
):
    """
    Manually test Odoo tracking write-back on a SINGLE order.
    - dry_run=true (default): only reads from Odoo, shows what would be updated, writes NOTHING
    - dry_run=false: actually writes carrier_tracking_ref to the picking
    """
    try:
        await odoo_connector.authenticate()

        # Step 1: Find the sale order
        sale_order_ids = odoo_connector.models.execute_kw(
            ODOO_DB, odoo_connector.uid, ODOO_API_KEY,
            'sale.order', 'search',
            [[('name', '=', order_name)]],
            {'limit': 1}
        )

        if not sale_order_ids:
            return {"status": "error", "message": f"Sale order {order_name} not found in Odoo"}

        # Step 2: Get pickings
        sale_order_data = odoo_connector.models.execute_kw(
            ODOO_DB, odoo_connector.uid, ODOO_API_KEY,
            'sale.order', 'read',
            [sale_order_ids],
            {'fields': ['name', 'state', 'picking_ids']}
        )

        sale_order = sale_order_data[0] if sale_order_data else {}
        picking_ids = sale_order.get('picking_ids', [])
        if not picking_ids:
            return {
                "status": "error",
                "message": f"No delivery orders found for {order_name}",
                "sale_order": {"id": sale_order_ids[0], "state": sale_order.get('state')}
            }

        # Step 3: Read picking details
        pickings = odoo_connector.models.execute_kw(
            ODOO_DB, odoo_connector.uid, ODOO_API_KEY,
            'stock.picking', 'read',
            [picking_ids],
            {'fields': ['name', 'picking_type_code', 'state', 'carrier_tracking_ref', 'origin']}
        )

        target_picking = None
        for picking in pickings:
            if picking.get('picking_type_code') == 'outgoing':
                target_picking = picking
                break
        if not target_picking and pickings:
            target_picking = pickings[-1]

        result = {
            "status": "success",
            "order_name": order_name,
            "sale_order_id": sale_order_ids[0],
            "sale_order_state": sale_order.get('state'),
            "pickings_found": len(pickings),
            "all_pickings": [
                {
                    "id": p['id'],
                    "name": p.get('name'),
                    "type": p.get('picking_type_code'),
                    "state": p.get('state'),
                    "current_tracking": p.get('carrier_tracking_ref') or "(empty)"
                }
                for p in pickings
            ],
            "target_picking": target_picking.get('name') if target_picking else None,
            "would_write": {
                "field": "carrier_tracking_ref",
                "value": tracking_number,
                "to_picking_id": target_picking['id'] if target_picking else None
            },
            "dry_run": dry_run
        }

        # Step 4: Actually write if dry_run=false
        if not dry_run and target_picking:
            odoo_connector.models.execute_kw(
                ODOO_DB, odoo_connector.uid, ODOO_API_KEY,
                'stock.picking', 'write',
                [[target_picking['id']], {'carrier_tracking_ref': tracking_number}]
            )
            result["written"] = True
            logger.info(f"TEST: Wrote tracking {tracking_number} to picking {target_picking['name']} for {order_name}")
        else:
            result["written"] = False

        return result

    except Exception as e:
        logger.error(f"Test Odoo tracking failed: {e}")
        return {"status": "error", "message": str(e)}


@app.post("/webhook/tracking")
async def tracking_webhook(request: Request, payload: dict):
    try:
        # Verify webhook signature/authentication
        auth_header = request.headers.get('X-Webhook-Secret')
        if not auth_header or not secrets.compare_digest(auth_header, WEBHOOK_SECRET):
            logger.warning(f"Unauthorized webhook attempt from {request.client.host}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid webhook secret"
            )

        logger.info(f"Received tracking webhook: {json.dumps(payload, indent=2)}")

        # Extract fields — handle both WebShip formats
        external_id = payload.get('external_id') or payload.get('orderId') or payload.get('shipmentReference')
        tracking_number = payload.get('tracking_number') or payload.get('trackingNumber') or payload.get('trackID')
        # Multi-leg shipments (e.g. FedEx SmartPost) may include a secondary
        # tracking number for the last-mile leg. Capture it under any of the
        # likely field names so we don't silently drop it.
        secondary_tracking = (
            payload.get('secondary_tracking')
            or payload.get('secondaryTrackingNumber')
            or payload.get('smart_post_number')
            or payload.get('smartPostNumber')
            or payload.get('subTrackingNumber')
            or payload.get('uspsTrackingNumber')
        )
        order_status = payload.get('status', 'shipped')
        carrier = payload.get('carrier', payload.get('carrierCode', 'FedEx'))
        service_code = payload.get('service') or payload.get('serviceCode') or payload.get('service_code') or ''

        if not external_id:
            logger.warning(f"Webhook missing order identifier: {payload}")
            return {"status": "error", "message": "Missing order identifier"}

        # Step 1: Update local database
        if db_manager.connection:
            cursor = db_manager.connection.cursor()
            if tracking_number:
                cursor.execute("""
                    UPDATE processed_orders
                    SET status = %s, tracking_number = %s, shipped_at = NOW()
                    WHERE order_name = %s
                """, (order_status, tracking_number, external_id))
            else:
                cursor.execute("""
                    UPDATE processed_orders
                    SET status = %s
                    WHERE order_name = %s
                """, (order_status, external_id))
            cursor.close()
            logger.info(f"DB updated for {external_id}: status={order_status}, tracking={tracking_number}")

        # Step 2: Write tracking back to Odoo (the critical part)
        odoo_updated = False
        if tracking_number and external_id:
            if not ODOO_WRITEBACK_ENABLED:
                logger.info(f"Odoo write-back SKIPPED (disabled): would update {external_id} → {tracking_number}")
            else:
                try:
                    odoo_updated = await odoo_connector.update_tracking_in_odoo(
                        order_name=external_id,
                        tracking_number=tracking_number,
                        carrier=carrier,
                        secondary_tracking=secondary_tracking,
                        service_code=service_code,
                    )
                except Exception as odoo_err:
                    # Log but don't fail the webhook — tracking is saved in our DB
                    logger.error(f"Odoo tracking update failed for {external_id} (non-fatal): {odoo_err}")

        return {
            "status": "success",
            "message": "Tracking update processed",
            "odoo_updated": odoo_updated
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Webhook processing error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)