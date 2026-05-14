"""
╔══════════════════════════════════════════════════════════════════╗
║     E-COMMERCE ANALYTICS PLATFORM — KAFKA CONSUMER              ║
║     Phase 1: Reads and displays messages from Kafka topics       ║
╚══════════════════════════════════════════════════════════════════╝
"""

import json
import sys
from kafka import KafkaConsumer
from datetime import datetime

# ─── COLOR CODES FOR TERMINAL OUTPUT ──────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RED    = "\033[91m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


def consume_topic(topic_name):
    """
    Connect to a Kafka topic and print every message that arrives.
    Run this in a separate terminal for each topic you want to watch.
    """
    print(f"\n{'='*60}")
    print(f"  👂 CONSUMER LISTENING ON TOPIC: {BOLD}{topic_name}{RESET}")
    print(f"  Waiting for messages... (Ctrl+C to stop)")
    print(f"{'='*60}\n")

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',       # Read only new messages
        enable_auto_commit=True,
        group_id=f'{topic_name}-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    message_count = 0

    try:
        for message in consumer:
            message_count += 1
            data = message.value

            print(f"{CYAN}[{datetime.now().strftime('%H:%M:%S')}]{RESET} "
                  f"Message #{message_count} | "
                  f"Partition: {message.partition} | "
                  f"Offset: {message.offset}")

            if topic_name == 'orders':
                print(f"  {GREEN}Order ID   :{RESET} {data.get('order_id')}")
                print(f"  {GREEN}Customer   :{RESET} {data.get('customer_name')} "
                      f"({data.get('customer_email')})")
                print(f"  {GREEN}City       :{RESET} {data.get('city')} "
                      f"({data.get('region')})")
                print(f"  {GREEN}Amount     :{RESET} ₹{data.get('final_amount'):,.2f} "
                      f"(discount: ₹{data.get('discount', 0):,.2f})")
                print(f"  {GREEN}Items      :{RESET} {data.get('num_items')} items | "
                      f"Device: {data.get('device')}")
                items_list = [f"{i['product_name']} x{i['quantity']}"
                              for i in data.get('items', [])]
                print(f"  {GREEN}Products   :{RESET} {', '.join(items_list)}")

            elif topic_name == 'payments':
                status_color = GREEN if data.get('status') == 'SUCCESS' else RED
                print(f"  {YELLOW}Payment ID :{RESET} {data.get('payment_id')}")
                print(f"  {YELLOW}Order ID   :{RESET} {data.get('order_id')}")
                print(f"  {YELLOW}Amount     :{RESET} ₹{data.get('amount'):,.2f}")
                print(f"  {YELLOW}Method     :{RESET} {data.get('payment_method')} "
                      f"via {data.get('gateway')}")
                print(f"  {YELLOW}Status     :{RESET} "
                      f"{status_color}{data.get('status')}{RESET}")

            elif topic_name == 'user_events':
                print(f"  {CYAN}Event ID   :{RESET} {data.get('event_id')}")
                print(f"  {CYAN}Event Type :{RESET} {data.get('event_type')}")
                print(f"  {CYAN}Customer   :{RESET} {data.get('customer_id')}")
                print(f"  {CYAN}Product    :{RESET} {data.get('product_name', 'N/A')}")
                print(f"  {CYAN}Device     :{RESET} {data.get('device')}")

            print()  # blank line between messages

    except KeyboardInterrupt:
        print(f"\n🛑 Consumer stopped. Total messages read: {message_count}")
        consumer.close()


def consume_all_topics():
    """
    Watch ALL three topics at once in a simple combined view.
    Good for a quick overview of everything happening.
    """
    print(f"\n{'='*60}")
    print(f"  👂 WATCHING ALL TOPICS: orders, payments, user_events")
    print(f"  Waiting for messages... (Ctrl+C to stop)")
    print(f"{'='*60}\n")

    consumer = KafkaConsumer(
        'orders', 'payments', 'user_events',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='all-topics-consumer',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    counts = {'orders': 0, 'payments': 0, 'user_events': 0}

    try:
        for message in consumer:
            topic = message.topic
            data  = message.value
            counts[topic] += 1

            if topic == 'orders':
                print(f"{GREEN}📦 ORDER{RESET}      | "
                      f"{data.get('order_id')} | "
                      f"{data.get('customer_name'):<20} | "
                      f"₹{data.get('final_amount'):>10,.2f} | "
                      f"{data.get('city')}")

            elif topic == 'payments':
                icon = "✅" if data.get('status') == 'SUCCESS' else "❌"
                print(f"{YELLOW}💳 PAYMENT{RESET}    | "
                      f"{data.get('payment_id')} | "
                      f"{data.get('payment_method'):<15} | "
                      f"₹{data.get('amount'):>10,.2f} | "
                      f"{icon} {data.get('status')}")

            elif topic == 'user_events':
                print(f"{CYAN}🖱️  USER EVENT{RESET} | "
                      f"{data.get('event_id')} | "
                      f"{data.get('event_type'):<20} | "
                      f"{data.get('customer_id')} | "
                      f"{data.get('device')}")

    except KeyboardInterrupt:
        print(f"\n\n{'='*60}")
        print(f"  🛑 Consumer stopped.")
        print(f"  Messages read → {counts}")
        print(f"{'='*60}")
        consumer.close()


# ─── ENTRY POINT ──────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) > 1:
        topic = sys.argv[1]
        if topic in ['orders', 'payments', 'user_events']:
            consume_topic(topic)
        else:
            print(f"Unknown topic: {topic}")
            print("Use: orders, payments, user_events, or no argument for all")
    else:
        consume_all_topics()