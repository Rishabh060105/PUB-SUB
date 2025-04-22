import tkinter as tk
from tkinter import ttk, scrolledtext
from confluent_kafka import Producer, Consumer, KafkaError
import threading
import uuid
import traceback

class KafkaPubSubGUI:
    def __init__(self, master, broker_ip):
        self.master = master
        self.broker_ip = broker_ip
        self.setup_gui()
        self.consumers = {}  # Track consumers by thread ID

    def setup_gui(self):
        self.master.title("Kafka PubSub Demo")

        # === PRODUCER SECTION ===
        ttk.Label(self.master, text="Producer Mode:").grid(row=0, column=0, padx=5, pady=5)
        self.producer_mode = ttk.Combobox(self.master, values=["Unicast", "Multicast", "Broadcast"])
        self.producer_mode.grid(row=0, column=1, padx=5, pady=5)
        self.producer_mode.set("Unicast")

        ttk.Label(self.master, text="Message:").grid(row=1, column=0, padx=5, pady=5)
        self.message_entry = ttk.Entry(self.master, width=40)
        self.message_entry.grid(row=1, column=1, padx=5, pady=5)

        self.premium_msg_flag = tk.BooleanVar()
        ttk.Checkbutton(self.master, text="Premium Message", variable=self.premium_msg_flag).grid(row=1, column=2, padx=5)

        self.send_btn = ttk.Button(self.master, text="Send Message", command=self.send_message)
        self.send_btn.grid(row=2, column=1, padx=5, pady=5)

        # === CONSUMER SECTION ===
        ttk.Label(self.master, text="Consumer Mode:").grid(row=3, column=0, padx=5, pady=5)
        self.consumer_mode = ttk.Combobox(self.master, values=["Unicast", "Multicast", "Broadcast"])
        self.consumer_mode.grid(row=3, column=1, padx=5, pady=5)
        self.consumer_mode.set("Unicast")

        self.premium_consumer_flag = tk.BooleanVar()
        ttk.Checkbutton(self.master, text="Premium Consumer", variable=self.premium_consumer_flag).grid(row=3, column=2, padx=5)

        self.start_consumer_btn = ttk.Button(self.master, text="Start Consumer", command=self.start_consumer)
        self.start_consumer_btn.grid(row=4, column=1, padx=5, pady=5)

        # === LOG AREA ===
        self.log_area = scrolledtext.ScrolledText(self.master, width=70, height=20)
        self.log_area.grid(row=5, column=0, columnspan=3, padx=5, pady=5)

    def log(self, message):
        self.log_area.insert(tk.END, message + "\n")
        self.log_area.see(tk.END)

    def send_message(self):
        mode = self.producer_mode.get().lower()
        message = self.message_entry.get().strip()
        is_premium = self.premium_msg_flag.get()

        if not message:
            self.log("[ERROR] Message cannot be empty.")
            return

        producer_config = {'bootstrap.servers': self.broker_ip}
        try:
            producer = Producer(producer_config)
            headers = []

            if mode == "multicast" and is_premium:
                headers = [('premium', b'true')]

            producer.produce(
                topic=f"{mode}_topic",
                value=message.encode('utf-8'),
                headers=headers
            )
            producer.flush()
            self.log(f"[PRODUCER] Sent ({mode}){' [PREMIUM]' if is_premium else ''}: {message}")

        except Exception as e:
            self.log(f"[ERROR] Failed to send message:\n{traceback.format_exc()}")

    def start_consumer(self):
        mode = self.consumer_mode.get().lower()
        is_premium_consumer = self.premium_consumer_flag.get()

        # Generate unique group ID per consumer mode
        group_id = {
            "unicast": "unicast_group",
            "multicast": f"multicast_group_{uuid.uuid4()}" if is_premium_consumer else "multicast_public",
            "broadcast": str(uuid.uuid4())
        }[mode]

        consumer_config = {
            'bootstrap.servers': self.broker_ip,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        }

        try:
            consumer = Consumer(consumer_config)
            consumer.subscribe([f"{mode}_topic"])

            thread = threading.Thread(
                target=self.consumer_loop,
                args=(consumer, mode, is_premium_consumer)
            )
            thread.daemon = True
            thread.start()
            self.consumers[thread.ident] = consumer
            self.log(f"[CONSUMER] Started in {mode} mode{' (PREMIUM)' if is_premium_consumer else ''}")

        except Exception as e:
            self.log(f"[ERROR] Failed to start consumer:\n{traceback.format_exc()}")

    def consumer_loop(self, consumer, mode, is_premium_consumer):
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    self.log(f"[ERROR] {msg.error()}")
                    continue

                # Decode headers
                headers = dict(msg.headers() or [])
                is_premium_msg = headers.get('premium', b'false') == b'true'

                # Filter based on consumer type
                if mode != "multicast" or (is_premium_msg and is_premium_consumer) or not is_premium_msg:
                    self.log(f"[CONSUMER] Received ({mode}){' [PREMIUM]' if is_premium_msg else ''}: {msg.value().decode()}")

        except Exception as e:
            self.log(f"[ERROR] Consumer crashed:\n{traceback.format_exc()}")
        finally:
            consumer.close()

if __name__ == "__main__":
    root = tk.Tk()
    broker_ip = input("Enter Kafka Broker IP (e.g., localhost:9092): ")
    app = KafkaPubSubGUI(root, broker_ip)
    root.mainloop()


