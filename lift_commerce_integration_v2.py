"""
Lift Commerce REST API Integration - WebShip Order Queue
Uses PUT Order endpoint to push orders into WebShip for manual shipping
"""

import requests
import logging
from datetime import datetime
from typing import Dict, Optional, List
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class OrderResult:
    """Result from PUT Order API"""
    success: bool
    order_id: Optional[str] = None
    error_message: Optional[str] = None


class LiftCommerceWebShipAPI:
    """
    Lift Commerce REST API client for WebShip integration
    Pushes orders into WebShip order queue instead of creating labels directly
    """

    def __init__(self, api_key: str, customer_id: str, integration_id: str, dry_run: bool = False):
        self.api_key = api_key
        self.customer_id = customer_id
        self.integration_id = integration_id
        self.base_url = "https://login.liftcommerce.io/restapi/v1"
        self.dry_run = dry_run
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        })

        if self.dry_run:
            logger.warning("🧪 DRY RUN MODE ENABLED - Orders will not be sent to WebShip")

    def put_order(
        self,
        order_id: str,
        order_number: str,
        order_date: str,
        sender: Dict,
        receiver: Dict,
        items: List[Dict],
        packages: Optional[List[Dict]] = None,
        shipping_total: float = 0.0,
        weight_unit: str = "lb",
        dim_unit: str = "in"
    ) -> OrderResult:
        """
        Push order to WebShip order queue using PUT Order API

        Args:
            order_id: Unique order ID (from Odoo)
            order_number: Order number (e.g., "AW153866")
            order_date: Order date (YYYY-MM-DD)
            sender: Sender address dict
            receiver: Receiver address dict
            items: List of order items
            packages: Optional pre-calculated packages
            shipping_total: Amount customer paid for shipping
            weight_unit: "lb" or "kg"
            dim_unit: "in" or "cm"

        Returns:
            OrderResult indicating success/failure
        """

        url = f"{self.base_url}/customers/{self.customer_id}/integrations/{self.integration_id}/orders/{order_id}"

        payload = {
            "orderId": str(order_id),
            "orderDate": order_date,
            "orderNumber": order_number,
            "fulfillmentStatus": "pending",
            "shippingService": "Standard",
            "shippingTotal": str(shipping_total),
            "weightUnit": weight_unit,
            "dimUnit": dim_unit,
            "dueByDate": None,
            "orderGroup": "Odoo Orders",
            "contentDescription": f"Order {order_number}",
            "sender": self._normalize_address(sender),
            "receiver": self._normalize_address(receiver),
            "items": items,
            "packages": packages if packages else []
        }

        try:
            # DRY RUN MODE: Simulate without calling API
            if self.dry_run:
                logger.warning(
                    f"🧪 DRY RUN: Would push order to WebShip\n"
                    f"   Order: {order_number} (ID: {order_id})\n"
                    f"   Receiver: {receiver.get('name')} in {receiver.get('city')}, {receiver.get('state')}\n"
                    f"   Items: {len(items)}\n"
                    f"   ⚠️  ORDER NOT SENT TO WEBSHIP"
                )
                return OrderResult(success=True, order_id=str(order_id))

            # PRODUCTION MODE: Call real API
            logger.info(f"Pushing order {order_number} to WebShip queue")

            response = self.session.put(url, json=payload, timeout=30)

            if response.status_code in [200, 201]:
                result = response.json()
                logger.info(f"✅ Order {order_number} pushed to WebShip successfully")
                return OrderResult(success=True, order_id=str(order_id))
            else:
                error_msg = f"API error {response.status_code}: {response.text}"
                logger.error(f"❌ {error_msg}")
                return OrderResult(success=False, error_message=error_msg)

        except requests.exceptions.Timeout:
            error_msg = "Request timed out after 30 seconds"
            logger.error(f"❌ {error_msg}")
            return OrderResult(success=False, error_message=error_msg)

        except Exception as e:
            error_msg = f"Exception: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return OrderResult(success=False, error_message=error_msg)

    def _normalize_address(self, address: Dict) -> Dict:
        """Normalize address to match PUT Order API requirements"""
        # Helper to convert Odoo False to empty string
        def safe_str(val):
            return "" if (val is None or val is False or val == False) else str(val)

        state = safe_str(address.get("state", ""))
        # Extract 2-letter state code if full name provided
        if len(state) > 2 and "(" in state:
            # "Oregon (US)" -> "OR"
            state = state.split("(")[0].strip()[:2].upper()
        elif len(state) > 2:
            state = state[:2].upper()

        # Phone must be 10+ digits or null
        phone = safe_str(address.get("phone", ""))
        if phone and len(phone) < 10:
            phone = phone.ljust(10, '0')  # Pad with zeros if too short
        if not phone:
            phone = "0000000000"  # Default placeholder

        # Email must be valid or null
        email = safe_str(address.get("email", ""))
        if not email or "@" not in email:
            email = "noreply@awdus.com"  # Default placeholder

        return {
            "name": safe_str(address.get("name", "")),
            "company": safe_str(address.get("company", "")),
            "address1": safe_str(address.get("address1") or address.get("street") or address.get("street1") or ""),
            "address2": safe_str(address.get("address2") or address.get("street2") or ""),
            "city": safe_str(address.get("city", "")),
            "state": state,
            "zip": safe_str(address.get("zip") or address.get("zip_code") or ""),
            "country": "US" if address.get("country") in ["United States", "US", "USA", None, "", False] else safe_str(address.get("country", "US"))[:2].upper(),
            "phone": phone,
            "email": email
        }

    def delete_order(self, order_id: str) -> bool:
        """Delete order from WebShip queue"""
        url = f"{self.base_url}/customers/{self.customer_id}/integrations/{self.integration_id}/orders/{order_id}"

        try:
            response = self.session.delete(url, timeout=15)
            if response.status_code in [200, 201]:
                logger.info(f"✅ Order {order_id} deleted from WebShip")
                return True
            else:
                logger.error(f"Failed to delete order {order_id}: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Error deleting order {order_id}: {e}")
            return False
