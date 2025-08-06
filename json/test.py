#!/usr/bin/env python3

import os
import sys
import time
import json
import random
import argparse
import threading
import concurrent.futures
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium_stealth import stealth
from google.oauth2.credentials import Credentials

# --- OAuth2 Client Config ---
CLIENT_CONFIG = {
    "web": {
        "client_id": "YOUR_CLIENT_ID",
        "project_id": "YOUR_PROJECT_ID",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_secret": "YOUR_CLIENT_SECRET",
        "redirect_uris": [
            "http://localhost:8080/oauth2callback",
            "http://localhost:8080/",
        ],
    }
}
SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

# --- Thread-Safe File Lock ---
file_lock = threading.Lock()


# --- HTTP Listener for OAuth2 callback ---
class OAuthCallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        qs = parse_qs(urlparse(self.path).query)
        code = qs.get("code", [None])[0]
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(
            b"<html><body><h1>Authentication successful</h1></body></html>"
        )
        self.server.auth_code = code
        threading.Thread(target=self.server.shutdown, daemon=True).start()


def start_http_listener():
    server = HTTPServer(("localhost", 0), OAuthCallbackHandler)
    server.auth_code = None
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


# --- Gmail OAuth2 Automator ---
class GmailOAuth2Automator:
    def __init__(self, headless=False, worker_id=None):
        self.headless = headless
        self.worker_id = worker_id
        self.success_count = 0
        self.failed_accounts = []
        self.debug_mode = True

    # --- Fingerprint Randomization ---
    def get_random_language(self):
        return random.choice(["en-US", "en", "fr", "es", "de", "ja"])

    def get_random_vendor(self):
        return random.choice(
            [
                "Google Inc.",
                "Mozilla Foundation",
                "Microsoft Corporation",
                "Apple Inc.",
                "Samsung Electronics",
                "IBM Corporation",
            ]
        )

    def get_random_platform(self):
        return random.choice(
            [
                "Win32",
                "Linux x86_64",
                "Macintosh",
                "Android",
                "iOS",
                "Windows NT",
                "Ubuntu",
                "Fedora",
                "Chrome OS",
            ]
        )

    def get_random_webgl_vendor(self):
        return random.choice(
            [
                "Intel Inc.",
                "NVIDIA Corporation",
                "AMD",
                "ARM",
                "Qualcomm",
                "Apple Inc.",
                "Broadcom",
                "Imagination Technologies",
            ]
        )

    def get_random_renderer(self):
        return random.choice(
            [
                "Intel Iris OpenGL Engine",
                "NVIDIA GeForce GTX",
                "AMD Radeon",
                "ARM Mali",
                "Qualcomm Adreno",
                "Apple A12 Bionic",
            ]
        )

    def get_random_timezone(self):
        return random.choice(
            [
                "US/Pacific",
                "US/Mountain",
                "US/Central",
                "US/Eastern",
                "Canada/Pacific",
                "Canada/Mountain",
                "Canada/Central",
                "Canada/Eastern",
            ]
        )

    def get_random_window_size(self):
        width = random.randint(1024, 1920)
        height = random.randint(768, 1080)
        return f"{width}x{height}"

    # --- Account Loader ---
    def load_gmail_accounts(self, file_path="gmail_accounts.txt"):
        accounts = []
        try:
            with open(file_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if ":" in line and "@" in line and not line.startswith("#"):
                        parts = line.split(":", 2)
                        if len(parts) >= 2:
                            email = parts[0].strip()
                            password = parts[1].strip()
                            totp_secret = parts[2].strip() if len(parts) > 2 else None
                            token_file = f"{email}.json"
                            accounts.append(
                                {
                                    "email": email,
                                    "password": password,
                                    "totp_secret": totp_secret,
                                    "token_file": token_file,
                                }
                            )
            return accounts
        except FileNotFoundError:
            print("❌ gmail_accounts.txt not found")
            return []

    # --- Logging ---
    def append_successful_account(self, account):
        line = f"{account['email']}:{account['password']}"
        if account.get("totp_secret"):
            line += f":{account['totp_secret']}"
        line += "\n"
        with file_lock:
            with open("success_accounts.txt", "a") as f:
                f.write(line)

    def append_failed_account(self, account, error_reason):
        line = f"{account['email']}:{account['password']}"
        if account.get("totp_secret"):
            line += f":{account['totp_secret']}"
        line += f" | ERROR: {error_reason}\n"
        with file_lock:
            with open("failed.txt", "a") as f:
                f.write(line)

    # --- Screenshot on Error ---
    def save_debug_screenshot(self, driver, email, page_type, error_reason=""):
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            worker_suffix = f"_W{self.worker_id}" if self.worker_id else ""
            screenshot_name = f"debug_{email.replace('@', '_')}_{page_type}_{timestamp}{worker_suffix}.png"
            driver.save_screenshot(screenshot_name)
            print(f"🖼️ Debug screenshot saved: {screenshot_name}")
        except Exception as e:
            print(f"❌ Failed to save screenshot: {e}")

    # --- Chrome Driver Factory ---
    def create_stealth_driver(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-extensions-file-access-check")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--allow-running-insecure-content")
        chrome_options.add_argument("--ignore-certificate-errors")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        chrome_options.add_argument("--window-size=" + self.get_random_window_size())
        chrome_options.add_argument("--force-timezone=" + self.get_random_timezone())
        if self.headless:
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.managed_default_content_settings.video": 2,
            "profile.default_content_setting_values.notifications": 2,
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
            "autofill.profile_enabled": False,
            "profile.pick_account_handler_behavior": "skip",
            "javascript.enabled": True,
        }
        chrome_options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(options=chrome_options)
        stealth(
            driver,
            languages=[self.get_random_language()],
            vendor=self.get_random_vendor(),
            platform=self.get_random_platform(),
            webgl_vendor=self.get_random_webgl_vendor(),
            renderer=self.get_random_renderer(),
        )
        return driver

    # --- OAuth2 URL Builder ---
    def build_oauth_url(self, port):
        import urllib.parse

        params = {
            "client_id": CLIENT_CONFIG["web"]["client_id"],
            "redirect_uri": f"http://localhost:{port}/oauth2callback",
            "scope": " ".join(SCOPES),
            "response_type": "code",
            "access_type": "offline",
            "prompt": "consent",
            "state": f"state_{random.randint(1000000, 9999999)}",
        }
        return f"https://accounts.google.com/o/oauth2/v2/auth?{urllib.parse.urlencode(params)}"

    # --- Main Automation Flow ---
    def automate_oauth_flow(self, email, password, totp_secret=None):
        driver = self.create_stealth_driver()
        wait = WebDriverWait(driver, 20)
        page_type = "init"
        error_reason = ""
        try:
            driver.get("https://www.example.com/")
            driver.implicitly_wait(10)
            time.sleep(1)
            listener = start_http_listener()
            port = listener.server.server_port
            oauth_url = self.build_oauth_url(port)
            driver.get(oauth_url)
            # Email entry
            email_field = wait.until(
                EC.element_to_be_clickable((By.ID, "identifierId"))
            )
            email_field.clear()
            email_field.send_keys(email)
            driver.find_element(By.ID, "identifierNext").click()
            driver.implicitly_wait(10)
            # Password entry
            password_field = wait.until(EC.element_to_be_clickable((By.NAME, "Passwd")))
            password_field.clear()
            password_field.send_keys(password)
            driver.find_element(By.ID, "passwordNext").click()
            driver.implicitly_wait(10)
            # TOTP 2FA if needed
            if totp_secret:
                import pyotp

                totp_input = wait.until(
                    EC.element_to_be_clickable(
                        (
                            By.CSS_SELECTOR,
                            'input[type="tel"],input[name="totpPin"],input[aria-label*="code"]',
                        )
                    )
                )
                totp_code = pyotp.TOTP(totp_secret).now()
                totp_input.clear()
                totp_input.send_keys(totp_code)
                driver.find_element(By.ID, "totpNext").click()
                time.sleep(2)
            # Unverified app warning
            time.sleep(2)
            page_source = driver.page_source.lower()
            if "unverified app" in page_source or "advanced" in page_source:
                try:
                    advanced = driver.find_element(
                        By.XPATH, "//button[contains(.,'Advanced')]"
                    )
                    advanced.click()
                    time.sleep(1)
                    unsafe = driver.find_element(
                        By.XPATH, "//a[contains(.,'Go to') or contains(.,'غير آمن')]"
                    )
                    unsafe.click()
                    time.sleep(1)
                except Exception:
                    pass
            # Consent screen
            for consent_xpath in [
                "//span[contains(text(), 'Continue')]",
                "//span[contains(text(), 'Allow')]",
                "//span[contains(text(), 'متابعة')]",
                "//button[@jsname='LgbsSe']",
                "//button[@id='submit_approve_access']",
                "//input[@value='Allow']",
            ]:
                try:
                    btn = wait.until(
                        EC.element_to_be_clickable((By.XPATH, consent_xpath))
                    )
                    btn.click()
                    time.sleep(2)
                    break
                except Exception:
                    continue
            # Wait for OAuth redirect
            timeout = time.time() + 90
            while time.time() < timeout and listener.auth_code is None:
                time.sleep(1)
            auth_code = listener.auth_code
            listener.server_close()
            if not auth_code:
                raise Exception("Timeout waiting for OAuth redirect")
            return auth_code
        except Exception as e:
            page_type = "error"
            error_reason = str(e)
            self.save_debug_screenshot(driver, email, page_type, error_reason)
            return None
        finally:
            driver.quit()

    # --- Token Exchange ---
    def exchange_code_for_tokens(self, auth_code, email, port):
        data = {
            "client_id": CLIENT_CONFIG["web"]["client_id"],
            "client_secret": CLIENT_CONFIG["web"]["client_secret"],
            "code": auth_code,
            "grant_type": "authorization_code",
            "redirect_uri": f"http://localhost:{port}/oauth2callback",
        }
        response = requests.post(
            CLIENT_CONFIG["web"]["token_uri"], data=data, timeout=30
        )
        if response.status_code == 200:
            token_data = response.json()
            creds = Credentials(
                token=token_data["access_token"],
                refresh_token=token_data.get("refresh_token"),
                token_uri=CLIENT_CONFIG["web"]["token_uri"],
                client_id=CLIENT_CONFIG["web"]["client_id"],
                client_secret=CLIENT_CONFIG["web"]["client_secret"],
                scopes=SCOPES,
            )
            return creds
        else:
            raise Exception(
                f"Token exchange failed: {response.status_code} - {response.text}"
            )

    # --- Save Token ---
    def save_token(self, credentials, token_file):
        with open(token_file, "w") as f:
            f.write(credentials.to_json())
        print(f"💾 Token saved to {token_file}")

    # --- Process Single Account ---
    def process_account(self, account):
        email = account["email"]
        password = account["password"]
        totp_secret = account.get("totp_secret")
        try:
            auth_code = self.automate_oauth_flow(email, password, totp_secret)
            if auth_code:
                port = 8080  # Use the default port for token exchange
                credentials = self.exchange_code_for_tokens(auth_code, email, port)
                self.append_successful_account(account)
                self.save_token(credentials, account["token_file"])
                self.success_count += 1
                print(f"🎉 Success for {email}")
                return True
            else:
                self.append_failed_account(
                    account, "OAuth flow failed - no authorization code obtained"
                )
                return False
        except Exception as e:
            self.append_failed_account(account, str(e))
            return False


# --- Worker for Parallel Processing ---
def process_accounts_worker(accounts_chunk, worker_id, headless):
    automator = GmailOAuth2Automator(headless=headless, worker_id=worker_id)
    for account in accounts_chunk:
        automator.process_account(account)
        time.sleep(random.uniform(2, 5))
    return {
        "worker_id": worker_id,
        "success_count": automator.success_count,
        "failed_count": len(automator.failed_accounts),
    }


# --- Main Entry Point ---
def main():
    parser = argparse.ArgumentParser(description="Gmail OAuth2 Token Generator")
    parser.add_argument(
        "--headless", action="store_true", help="Run Chrome in headless mode"
    )
    parser.add_argument(
        "--workers", type=int, default=1, help="Number of parallel workers (default: 1)"
    )
    args = parser.parse_args()
    automator = GmailOAuth2Automator(headless=args.headless)
    accounts = automator.load_gmail_accounts()
    if not accounts:
        print("❌ No valid accounts found")
        return
    print(f"📧 Found {len(accounts)} accounts")
    print("Proceed with automation? (y/n):", end=" ", flush=True)
    response = input().strip().lower()
    if response != "y":
        print("❌ Automation cancelled")
        return
    if os.path.exists("failed.txt"):
        os.remove("failed.txt")
    if args.workers == 1:
        automator.process_all_accounts()
    else:
        chunk_size = max(1, len(accounts) // args.workers)
        account_chunks = [
            accounts[i : i + chunk_size] for i in range(0, len(accounts), chunk_size)
        ]
        while len(account_chunks) > args.workers:
            account_chunks[-2].extend(account_chunks[-1])
            account_chunks.pop()
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=args.workers
        ) as executor:
            futures = {
                executor.submit(process_accounts_worker, chunk, i + 1, args.headless): i
                + 1
                for i, chunk in enumerate(account_chunks)
            }
            for future in concurrent.futures.as_completed(futures):
                worker_id = futures[future]
                try:
                    result = future.result()
                    print(
                        f"✅ Worker {worker_id} completed: {result['success_count']} success, {result['failed_count']} failed"
                    )
                except Exception as exc:
                    print(f"❌ Worker {worker_id} generated an exception: {exc}")
    print("\n🎉 Automation completed!")
    print("💾 Token files are ready for your Gmail bulk sender")


if __name__ == "__main__":
    main()
