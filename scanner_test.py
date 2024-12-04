import socket
import time
import threading
import paho.mqtt.client as mqtt

def on_connect(client, userdata, flags, rc):
    """Callback for when the client connects to the MQTT broker."""
    if rc == 0:
        print("MQTT Connected successfully.")
    else:
        print(f"MQTT Connection failed with code {rc}.")

def on_disconnect(client, userdata, rc):
    """Callback for when the client disconnects from the MQTT broker."""
    print("MQTT Disconnected. Attempting to reconnect...")
    while True:
        try:
            client.reconnect()
            print("MQTT Reconnected successfully.")
            break
        except Exception as e:
            print(f"Reconnection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def publish_with_reconnect(mqtt_client, topic, message):
    """Publish a message to a topic, ensuring reconnection if necessary."""
    try:
        mqtt_client.publish(topic, message)
        print(f"Published to MQTT topic '{topic}': {message}")
    except Exception as e:
        print(f"Publish failed: {e}. Ensuring MQTT connection...")
        mqtt_client.reconnect()

def read_from_socket_and_publish(ip, port, mqtt_client, topic, reconnect_delay=5):
    """
    Reads data from a socket connection to a specified IP and port, and publishes it to an MQTT topic.
    """
    while True:
        try:
            # Create a socket object
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                print(f"[{ip}:{port}] Attempting to connect...")
                sock.connect((ip, port))
                print(f"[{ip}:{port}] Connected.")
                
                # Receive data from the server
                while True:
                    data = sock.recv(1024)
                    if not data:
                        print(f"[{ip}:{port}] No data received. Server might have closed the connection.")
                        break
                    decoded_data = data.decode('utf-8')
                    print(f"[{ip}:{port}] Received: {decoded_data}")
                    
                    # Publish data to MQTT
                    publish_with_reconnect(mqtt_client, topic, decoded_data)
        
        except (ConnectionRefusedError, socket.error) as e:
            print(f"[{ip}:{port}] Connection error: {e}. Retrying in {reconnect_delay} seconds...")
            time.sleep(reconnect_delay)
        except KeyboardInterrupt:
            print(f"[{ip}:{port}] Connection closed by user.")
            break
        except Exception as e:
            print(f"[{ip}:{port}] Unexpected error: {e}")
            break

def main():
    # MQTT broker configuration
    broker_address = "15.206.108.18"  # Replace with your MQTT broker address
    broker_port = 1883                   # Replace with your MQTT broker port (1883 is default for non-TLS)
    #username = "mqtt_user"               # Replace with your MQTT username (if applicable)
    #password = "mqtt_password"           # Replace with your MQTT password (if applicable)
    
    # Initialize MQTT client with persistent session
    mqtt_client = mqtt.Client(client_id="ucal_i1", clean_session=False)
    #mqtt_client.username_pw_set(username, password)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect

    # Attempt initial connection to MQTT broker
    while True:
        try:
            mqtt_client.connect(broker_address, broker_port, 60)
            mqtt_client.loop_start()
            break
        except Exception as e:
            print(f"MQTT initial connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)
    
    # List of devices with their IPs, ports, and topics
    scanner_devices = [
        {"ip": "192.168.0.7", "port": 23, "topic": "scanner/device1/data"},
        {"ip": "192.168.0.102", "port": 23, "topic": "scanner/device2/data"},
        {"ip": "192.168.0.103", "port": 23, "topic": "scanner/device3/data"},
        {"ip": "192.168.0.104", "port": 23, "topic": "scanner/device4/data"},
        {"ip": "192.168.0.105", "port": 23, "topic": "scanner/device5/data"},
    ]
    
    threads = []
    
    # Start a thread for each scanner device
    for device in scanner_devices:
        ip = device["ip"]
        port = device["port"]
        topic = device["topic"]
        thread = threading.Thread(target=read_from_socket_and_publish, args=(ip, port, mqtt_client, topic))
        thread.daemon = True
        threads.append(thread)
        thread.start()
    
    # Keep the main program alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down all connections.")
        mqtt_client.loop_stop()  # Stop MQTT client loop

if __name__ == "__main__":
    main()
