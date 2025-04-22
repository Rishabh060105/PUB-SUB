
import uuid
import threading
import tkinter as tk
from tkinter import ttk, scrolledtext, simpledialog, messagebox
from confluent_kafka import Producer,Consumer,KafkaError

class KafkaPubSubGUI:
    def __init__(self, master: tk.Tk, broker_ip: str) -> None:
        self.master = master
        self.broker_ip = broker_ip
        self.master.protocol("WM_DELETE_WINDOW", self._on_close)
        self.consumers = {}
        self.multicast_index = 0
        self._build_gui()

    def _build_gui(self) -> None:
        self.master.title("Kafka Pub/Sub Demo")

        # Producer Section
        ttk.Label(self.master, text="Producer").grid(row=0, column=0, pady=5, sticky="w")
        self.producer_mode = ttk.Combobox(
            self.master,
            values=["Unicast", "Multicast", "Broadcast"],
            state="readonly"
        )
        self.producer_mode.grid(row=1, column=0, padx=5)
        self.producer_mode.set("Unicast")

        self.message_entry = ttk.Entry(self.master, width=30)
        self.message_entry.grid(row=1, column=1, padx=5)

        ttk.Button(
            self.master,
            text="Send",
            command=self.send_message
        ).grid(row=1, column=2, padx=5)

        # Consumer Section
        ttk.Label(self.master, text="Consumer").grid(row=2, column=0, pady=5, sticky="w")
        self.consumer_mode = ttk.Combobox(
            self.master,
            values=["Unicast", "Multicast", "Broadcast"],
            state="readonly"
        )
        self.consumer_mode.grid(row=3, column=0, padx=5)
        self.consumer_mode.set("Unicast")

        ttk.Button(
            self.master,
            text="Start Consumer",
            command=self.start_consumer
        ).grid(row=3, column=1, padx=5)

        # Log Area
        self.log_area = scrolledtext.ScrolledText(
            self.master,
            width=70,
            height=15,
            state="disabled"
        )
        self.log_area.grid(row=4, column=0, columnspan=3, padx=5, pady=5)

    def log(self, message: str) -> None:
        self.log_area.config(state="normal")
        self.log_area.insert(tk.END, message + "\n")
        self.log_area.see(tk.END)
        self.log_area.config(state="disabled")

    def send_message(self) -> None:
        mode = self.producer_mode.get().lower()
        message = self.message_entry.get().strip()
        if not message:
            self.log("[ERROR] Cannot send an empty message.")
            return

        topic = f"{mode}_topic"
        try:
            producer = Producer({'bootstrap.servers': self.broker_ip})
            producer.produce(topic, message.encode('utf-8'))
            producer.flush()
            self.log(f"[PRODUCER] Sent ({mode}): {message}")
        except Exception as e:
            self.log(f"[ERROR] Failed to send message: {e}")

    def start_consumer(self) -> None:
        mode = self.consumer_mode.get()
        topic = f"{mode.lower()}_topic"

        # Determine unique group id
        if mode == "Unicast":
            group_id = "unicast_group"
        elif mode == "Multicast":
            group_id = f"multicast_group_{self.multicast_index}"
            self.multicast_index += 1
        else:
            group_id = str(uuid.uuid4())

        try:
            consumer = Consumer({
                'bootstrap.servers': self.broker_ip,
                'group.id': group_id,
                'auto.offset.reset': 'earliest'
            })
            consumer.subscribe([topic])

            thread = threading.Thread(
                target=self._consumer_loop,
                args=(consumer, group_id, mode.lower()),
                daemon=True
            )
            thread.start()
            self.consumers[group_id] = consumer
            self.log(f"[CONSUMER] Started {mode} consumer with group '{group_id}' on topic '{topic}'.")
        except Exception as e:
            self.log(f"[ERROR] Failed to start consumer: {e}")

    def _consumer_loop(self, consumer: Consumer, consumer_id: str, mode: str) -> None:
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    # Partition EOF is not an error
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    self.log(f"[ERROR] Consumer {consumer_id}: {msg.error()}")
                    continue

                text = msg.value().decode('utf-8', errors='replace')
                self.log(f"[CONSUMER] ({mode}) {text}")
        except Exception as e:
            self.log(f"[ERROR] Consumer {consumer_id} loop stopped: {e}")
        finally:
            consumer.close()

    def _on_close(self) -> None:
        # Cleanly close all consumers
        for cid, consumer in self.consumers.items():
            try:
                consumer.close()
            except Exception:
                pass
        self.master.destroy()

if __name__ == '__main__':
    root = tk.Tk()
    # Prompt for broker IP via dialog instead of console input
    broker_ip = simpledialog.askstring(
        "Broker Address",
        "Enter Kafka broker (e.g. 127.0.0.1:9092):",
        parent=root
    )
    if not broker_ip:
        messagebox.showerror("Error", "Broker address is required.")
        root.destroy()
    else:
        app = KafkaPubSubGUI(root, broker_ip)
        root.mainloop()

