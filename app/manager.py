import json
import os
import logging
from datetime import datetime

base_dir = "/config" if os.path.exists("/config") and os.access("/config", os.W_OK) else os.path.join(os.getcwd(), "config")

if not os.path.exists(base_dir):
    os.makedirs(base_dir)

CONFIG_PATH = os.path.join(base_dir, "config.json")
LOG_DIR = os.path.join(base_dir, "logs")

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

class ConfigManager:
    def __init__(self):
        self.ensure_config()
    
    def ensure_config(self):
        if not os.path.exists(CONFIG_PATH):
            default_config = {
                "server_url": "https://fox.oplist.org",
                "username": "",
                "password": "",
                "tasks": [],
                "tool_password": os.getenv("TOOL_PASSWORD", "admin888")
            }
            self.save_config(default_config)

    def get_config(self):
        with open(CONFIG_PATH, 'r') as f:
            data = json.load(f)
            # 兼容旧版本
            if "tool_password" not in data:
                data["tool_password"] = os.getenv("TOOL_PASSWORD", "admin888")
            return data

    def save_config(self, config):
        with open(CONFIG_PATH, 'w') as f:
            json.dump(config, f, indent=4)

    def add_log(self, task_name, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_file = os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.log")
        with open(log_file, "a") as f:
            f.write(f"[{timestamp}] [{task_name}] {message}\n")

    def get_logs(self):
        logs = []
        if not os.path.exists(LOG_DIR):
            return logs
        log_files = sorted(os.listdir(LOG_DIR), reverse=True)
        if not log_files:
            return logs
        with open(os.path.join(LOG_DIR, log_files[0]), 'r') as f:
            lines = f.readlines()
            logs = lines[-100:]
        return logs

manager = ConfigManager()
