"""
Lift Commerce Core API Integration
Book Shipment method - creates shipping labels automatically

Based on successful Core API test (Oct 26, 2025)
Book Number: 34621068, Tracking: 61290386139620648393
"""

import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ShipmentResult:
    """Result from Book Shipment API"""
    success: bool
    book_number: Optional[str] = None
    tracking_number: Optional[str] = None
    carrier_code: Optional[str] = None
    service_code: Optional[str] = None
    error_message: Optional[str] = None


class LiftCommerceAPI:
    """
    Lift Commerce Core API client
    Uses Book Shipment endpoint for automated label creation
    """

    def __init__(self, api_key: str, customer_id: str, dry_run: bool = False):
        self.api_key = api_key
        self.customer_id = customer_id
        self.base_url = "https://login.liftcommerce.io/restapi/v1"
        self.dry_run = dry_run
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        })

        if self.dry_run:
            logger.warning("🧪 DRY RUN MODE ENABLED - No real labels will be created")

    def book_shipment(
        self,
        order_reference: str,
        sender: Dict,
        receiver: Dict,
        packages: list,
        service_code: str = "fedex_smart_post",
        shipment_date: Optional[str] = None
    ) -> ShipmentResult:
        """
        Create shipping label using Core API Book Shipment endpoint

        Args:
            order_reference: Your order ID (e.g., "ODOO-12345")
            sender: Sender address dict with all required fields
            receiver: Receiver address dict with all required fields
            packages: List of package dicts with weight, dimensions
            service_code: FedEx service (default: fedex_smart_post)
            shipment_date: Ship date (default: tomorrow)

        Returns:
            ShipmentResult with book_number and tracking_number
        """

        url = f"{self.base_url}/customers/{self.customer_id}/shipments"

        # Default to tomorrow's date
        if not shipment_date:
            shipment_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

        # Build payload with all required fields
        payload = {
            "carrierCode": "fedex",
            "serviceCode": service_code,
            "packageTypeCode": "fedex_custom_package",
            "shipmentDate": shipment_date,
            "shipmentReference": order_reference,
            "contentDescription": f"Order {order_reference}",
            "sender": self._normalize_address(sender, is_sender=True),
            "receiver": self._normalize_address(receiver, is_sender=False),
            "residential": receiver.get("residential", True),
            "signatureOptionCode": "NO_SIGNATURE_REQUIRED",
            "weightUnit": "lb",
            "dimUnit": "in",
            "currency": "USD",
            "customsCurrency": "USD",
            "pieces": self._normalize_packages(packages, service_code)
        }

        try:
            # DRY RUN MODE: Simulate without calling API
            if self.dry_run:
                import random
                mock_book_number = f"DRY-{random.randint(10000000, 99999999)}"
                mock_tracking = f"DRY{random.randint(10000000000000000000, 99999999999999999999)}"

                logger.warning(
                    f"🧪 DRY RUN: Simulated label for {order_reference}\n"
                    f"   Book #: {mock_book_number}\n"
                    f"   Tracking: {mock_tracking}\n"
                    f"   Service: {service_code}\n"
                    f"   ⚠️  NO REAL LABEL CREATED"
                )

                return ShipmentResult(
                    success=True,
                    book_number=mock_book_number,
                    tracking_number=mock_tracking,
                    carrier_code='fedex',
                    service_code=service_code
                )

            # PRODUCTION MODE: Call real API
            logger.info(f"Booking shipment for {order_reference} with {service_code}")

            response = self.session.post(url, json=payload, timeout=30)

            if response.status_code in [200, 201]:
                result = response.json()

                logger.info(
                    f"✅ Label created: Book #{result.get('bookNumber')}, "
                    f"Tracking: {result.get('trackingNumber')}"
                )

                return ShipmentResult(
                    success=True,
                    book_number=str(result.get('bookNumber')),
                    tracking_number=result.get('trackingNumber'),
                    carrier_code=result.get('carrierCode', 'fedex'),
                    service_code=result.get('serviceCode', service_code)
                )
            else:
                error_msg = f"API error {response.status_code}: {response.text}"
                logger.error(f"❌ {error_msg}")

                return ShipmentResult(
                    success=False,
                    error_message=error_msg
                )

        except requests.exceptions.Timeout:
            error_msg = "Request timed out after 30 seconds"
            logger.error(f"❌ {error_msg}")
            return ShipmentResult(success=False, error_message=error_msg)

        except Exception as e:
            error_msg = f"Exception: {str(e)}"
            logger.error(f"❌ {error_msg}")
            return ShipmentResult(success=False, error_message=error_msg)

    def _normalize_address(self, address: Dict, is_sender: bool) -> Dict:
        """
        Normalize address to match API requirements
        All fields required, use empty string if not provided
        """
        return {
            "name": address.get("name", ""),
            "company": address.get("company", ""),
            "address1": address.get("address1", address.get("street", "")),
            "address2": address.get("address2", address.get("street2", "")),
            "city": address.get("city", ""),
            "state": address.get("state", ""),
            "zip": address.get("zip", address.get("zip_code", "")),
            "country": address.get("country", "US"),
            "phone": address.get("phone", ""),
            "email": address.get("email", "")
        }

    def _normalize_packages(self, packages: list, service_code: str) -> list:
        """
        Normalize packages to match API requirements
        SmartPost requires insuranceAmount = "0"
        """
        normalized = []

        for pkg in packages:
            # Determine if insurance is allowed
            # SmartPost DOES NOT allow insurance
            insurance_amount = "0" if "smart_post" in service_code.lower() else str(pkg.get("insurance", 0))

            normalized.append({
                "weight": str(pkg.get("weight", 1.0)),
                "length": str(pkg.get("length", 10.0)),
                "width": str(pkg.get("width", 8.0)),
                "height": str(pkg.get("height", 6.0)),
                "insuranceAmount": insurance_amount,
                "declaredValue": str(pkg.get("value", pkg.get("declared_value", 50.0)))
            })

        return normalized

    def get_label_url(self, book_number: str, format: str = "PDF") -> str:
        """Get URL to download shipping label"""
        return f"{self.base_url}/customers/{self.customer_id}/shipments/{book_number}/label/{format}"

    def download_label(self, book_number: str, save_path: str) -> bool:
        """Download shipping label PDF"""
        try:
            url = self.get_label_url(book_number)
            response = self.session.get(url, timeout=30)

            if response.status_code == 200:
                with open(save_path, 'wb') as f:
                    f.write(response.content)
                logger.info(f"Label downloaded: {save_path}")
                return True
            else:
                logger.error(f"Failed to download label: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Error downloading label: {e}")
            return False


# Example usage
if __name__ == "__main__":
    import os

    # Initialize API client
    api = LiftCommerceAPI(
        api_key=os.getenv("LIFT_API_KEY", "pspgUG9g0ykzTTZ8QYqVxWsj4VOdZlZk"),
        customer_id=os.getenv("LIFT_CUSTOMER_ID", "33800193")
    )

    # Example sender (AWDS warehouse)
    sender = {
        "name": "AWDS Warehouse",
        "company": "AWDS Company",
        "address1": "123 Warehouse St",
        "address2": "",
        "city": "Salt Lake City",
        "state": "UT",
        "zip": "84106",
        "country": "US",
        "phone": "8015551234",
        "email": "warehouse@awds.com"
    }

    # Example receiver
    receiver = {
        "name": "John Doe",
        "company": "",
        "address1": "456 Customer Ave",
        "address2": "",
        "city": "New York",
        "state": "NY",
        "zip": "10001",
        "country": "US",
        "phone": "2125551234",
        "email": "john@customer.com",
        "residential": True
    }

    # Example packages
    packages = [
        {
            "weight": 2.5,
            "length": 10,
            "width": 8,
            "height": 6,
            "value": 50.00
        }
    ]

    # Book shipment
    print("Testing Lift Commerce API Integration")
    print("="*60)

    result = api.book_shipment(
        order_reference="TEST-INTEGRATION-001",
        sender=sender,
        receiver=receiver,
        packages=packages,
        service_code="fedex_smart_post"  # Ray's PRIMARY choice
    )

    if result.success:
        print(f"✅ SUCCESS!")
        print(f"   Book Number: {result.book_number}")
        print(f"   Tracking: {result.tracking_number}")
        print(f"   Service: {result.service_code}")
    else:
        print(f"❌ FAILED: {result.error_message}")
