"""
╔══════════════════════════════════════════════════════════════════╗
║     E-COMMERCE ANALYTICS PLATFORM — KAFKA PRODUCER              ║
║     Phase 1: Simulates a live e-commerce data stream            ║
║     Topics: orders, payments, user_events                       ║
╚══════════════════════════════════════════════════════════════════╝
"""

import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

# ─── SETUP ────────────────────────────────────────────────────────
fake = Faker('en_IN')   # Indian locale for realistic data

# Connect to Kafka
# We retry a few times in case Kafka is still starting up
def create_producer():
    retries = 10
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            print("✅ Connected to Kafka successfully!")
            return producer
        except Exception as e:
            print(f"⏳ Waiting for Kafka... attempt {i+1}/{retries} ({e})")
            time.sleep(5)
    raise Exception("❌ Could not connect to Kafka after multiple attempts")


# ─── REFERENCE DATA ────────────────────────────────────────────────
# Realistic product catalogue
PRODUCTS = [
    {"product_id": "P001", "name": "iPhone 15 Pro",        "category": "Electronics",  "price": 134900},
    {"product_id": "P002", "name": "Samsung 65\" QLED TV", "category": "Electronics",  "price": 89999},
    {"product_id": "P003", "name": "Nike Air Max",         "category": "Footwear",     "price": 9995},
    {"product_id": "P004", "name": "Levi's 511 Jeans",     "category": "Clothing",     "price": 3999},
    {"product_id": "P005", "name": "Harry Potter Box Set", "category": "Books",        "price": 2499},
    {"product_id": "P006", "name": "Instant Pot Duo",      "category": "Kitchen",      "price": 7999},
    {"product_id": "P007", "name": "Yoga Mat Premium",     "category": "Sports",       "price": 1299},
    {"product_id": "P008", "name": "MacBook Air M2",       "category": "Electronics",  "price": 114900},
    {"product_id": "P009", "name": "Boat Airdopes 141",    "category": "Electronics",  "price": 1299},
    {"product_id": "P010", "name": "Prestige Cooker 5L",   "category": "Kitchen",      "price": 1850},
    {"product_id": "P011", "name": "Adidas Running Shoes", "category": "Footwear",     "price": 6999},
    {"product_id": "P012", "name": "Atomic Habits Book",   "category": "Books",        "price": 499},
    {"product_id": "P013", "name": "Campus T-Shirt Pack",  "category": "Clothing",     "price": 899},
    {"product_id": "P014", "name": "OnePlus 12",           "category": "Electronics",  "price": 64999},
    {"product_id": "P015", "name": "Whey Protein 2kg",     "category": "Sports",       "price": 3499},
]

REGIONS = ["North", "South", "East", "West", "Central"]
CITIES  = {
    "North":   ["Delhi", "Chandigarh", "Lucknow", "Jaipur"],
    "South":   ["Chennai", "Bangalore", "Hyderabad", "Kochi"],
    "East":    ["Kolkata", "Bhubaneswar", "Patna", "Guwahati"],
    "West":    ["Mumbai", "Pune", "Ahmedabad", "Surat"],
    "Central": ["Bhopal", "Nagpur", "Indore", "Raipur"],
}
PAYMENT_METHODS  = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash on Delivery", "Wallet"]
DEVICES          = ["Mobile", "Desktop", "Tablet"]
ORDER_STATUSES   = ["PLACED", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
EVENT_TYPES      = ["PAGE_VIEW", "PRODUCT_CLICK", "ADD_TO_CART", "REMOVE_FROM_CART",
                    "WISHLIST_ADD", "SEARCH", "CHECKOUT_START", "COUPON_APPLIED"]


# ─── EVENT GENERATORS ──────────────────────────────────────────────

def generate_customer_id():
    """Generate a realistic customer ID"""
    return f"CUST{random.randint(1000, 9999)}"

def generate_order(customer_id=None):
    """
    Generate a realistic order event.
    This is what gets sent to the 'orders' Kafka topic.
    """
    if not customer_id:
        customer_id = generate_customer_id()

    region = random.choice(REGIONS)
    city   = random.choice(CITIES[region])

    # Pick 1 to 4 products per order
    num_items   = random.randint(1, 4)
    items       = random.sample(PRODUCTS, num_items)
    order_items = []
    total_amount = 0

    for item in items:
        quantity    = random.randint(1, 3)
        item_total  = item["price"] * quantity
        total_amount += item_total
        order_items.append({
            "product_id":   item["product_id"],
            "product_name": item["name"],
            "category":     item["category"],
            "unit_price":   item["price"],
            "quantity":     quantity,
            "item_total":   item_total
        })

    # Apply a discount sometimes
    discount = 0
    if random.random() < 0.3:   # 30% chance of discount
        discount = round(total_amount * random.uniform(0.05, 0.20), 2)

    final_amount = round(total_amount - discount, 2)

    order = {
        "order_id":       f"ORD{uuid.uuid4().hex[:8].upper()}",
        "customer_id":    customer_id,
        "customer_name":  fake.name(),
        "customer_email": fake.email(),
        "customer_phone": fake.phone_number(),
        "region":         region,
        "city":           city,
        "address":        fake.address().replace('\n', ', '),
        "items":          order_items,
        "num_items":      num_items,
        "subtotal":       total_amount,
        "discount":       discount,
        "final_amount":   final_amount,
        "device":         random.choice(DEVICES),
        "status":         "PLACED",
        "timestamp":      datetime.now().isoformat(),
        "date":           datetime.now().strftime("%Y-%m-%d"),
        "hour":           datetime.now().hour,
    }
    return order


def generate_payment(order_id, customer_id, amount):
    """
    Generate a payment event linked to an order.
    Sent to the 'payments' Kafka topic.
    """
    # 90% payments succeed, 10% fail
    success = random.random() < 0.9

    payment = {
        "payment_id":     f"PAY{uuid.uuid4().hex[:8].upper()}",
        "order_id":       order_id,
        "customer_id":    customer_id,
        "amount":         amount,
        "payment_method": random.choice(PAYMENT_METHODS),
        "status":         "SUCCESS" if success else "FAILED",
        "gateway":        random.choice(["Razorpay", "Paytm", "PayU", "CCAvenue"]),
        "transaction_id": f"TXN{uuid.uuid4().hex[:10].upper()}",
        "timestamp":      datetime.now().isoformat(),
        "date":           datetime.now().strftime("%Y-%m-%d"),
    }
    return payment


def generate_user_event(customer_id=None):
    """
    Generate a user behaviour event (clicks, searches, page views).
    Sent to the 'user_events' Kafka topic.
    """
    if not customer_id:
        customer_id = generate_customer_id()

    product = random.choice(PRODUCTS)
    event_type = random.choice(EVENT_TYPES)

    event = {
        "event_id":      f"EVT{uuid.uuid4().hex[:8].upper()}",
        "customer_id":   customer_id,
        "event_type":    event_type,
        "product_id":    product["product_id"] if event_type != "SEARCH" else None,
        "product_name":  product["name"]       if event_type != "SEARCH" else None,
        "category":      product["category"]   if event_type != "SEARCH" else None,
        "search_query":  fake.word()            if event_type == "SEARCH" else None,
        "device":        random.choice(DEVICES),
        "session_id":    f"SES{uuid.uuid4().hex[:6].upper()}",
        "page_url":      f"/product/{product['product_id'].lower()}",
        "timestamp":     datetime.now().isoformat(),
        "date":          datetime.now().strftime("%Y-%m-%d"),
    }
    return event


# ─── MAIN PRODUCER LOOP ────────────────────────────────────────────

def run_producer():
    producer = create_producer()

    print("\n" + "="*60)
    print("  🚀 E-COMMERCE KAFKA PRODUCER STARTED")
    print("  Streaming data to topics: orders, payments, user_events")
    print("="*60 + "\n")

    orders_sent    = 0
    payments_sent  = 0
    events_sent    = 0

    try:
        while True:

            # ── Generate an Order ──────────────────────────────────
            customer_id = generate_customer_id()
            order       = generate_order(customer_id)

            producer.send(
                topic = 'orders',
                key   = order['order_id'],
                value = order
            )
            orders_sent += 1

            print(f"📦 ORDER   | {order['order_id']} | "
                  f"{order['customer_name']:<20} | "
                  f"₹{order['final_amount']:>10,.2f} | "
                  f"{order['city']:<12} | "
                  f"{order['num_items']} items")

            # ── Generate a Payment for this Order ─────────────────
            payment = generate_payment(
                order['order_id'],
                customer_id,
                order['final_amount']
            )

            producer.send(
                topic = 'payments',
                key   = payment['payment_id'],
                value = payment
            )
            payments_sent += 1

            status_icon = "✅" if payment['status'] == "SUCCESS" else "❌"
            print(f"💳 PAYMENT | {payment['payment_id']} | "
                  f"{payment['payment_method']:<15} | "
                  f"₹{payment['amount']:>10,.2f} | "
                  f"{status_icon} {payment['status']}")

            # ── Generate 2-5 User Events ──────────────────────────
            num_events = random.randint(2, 5)
            for _ in range(num_events):
                event = generate_user_event(customer_id)
                producer.send(
                    topic = 'user_events',
                    key   = event['event_id'],
                    value = event
                )
                events_sent += 1

            # Print a running total every 10 orders
            if orders_sent % 10 == 0:
                print(f"\n{'─'*60}")
                print(f"  📊 STATS → Orders: {orders_sent} | "
                      f"Payments: {payments_sent} | "
                      f"Events: {events_sent}")
                print(f"{'─'*60}\n")

            # Flush to make sure messages are sent
            producer.flush()

            # Wait before next order (0.5 to 1.5 seconds)
            time.sleep(random.uniform(0.5, 1.5))

    except KeyboardInterrupt:
        print(f"\n\n{'='*60}")
        print(f"  🛑 Producer stopped by user")
        print(f"  Total sent → Orders: {orders_sent} | "
              f"Payments: {payments_sent} | "
              f"Events: {events_sent}")
        print(f"{'='*60}")
        producer.close()


# ─── ENTRY POINT ──────────────────────────────────────────────────
if __name__ == "__main__":
    run_producer()