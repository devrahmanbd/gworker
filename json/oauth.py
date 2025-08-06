#!/usr/bin/env python3
import time, sys, random, os, json, requests, argparse, concurrent.futures, threading
from datetime import datetime
from urllib.parse import parse_qs, urlparse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
from google.oauth2.credentials import Credentials

CLIENT_CONFIG = {"web":{"client_id":"971405107939-81h7arv9fv5o8eid4k94792re3ea54pn.apps.googleusercontent.com","project_id":"jul-26-467111","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs","client_secret":"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","redirect_uris":["http://localhost:8080/","http://localhost:8080/oauth2callback"]}}
SCOPES = ['https://www.googleapis.com/auth/gmail.send']
file_lock = threading.Lock()
captcha_lock = threading.Lock()

class GmailOAuth2Automator:
    def __init__(self, headless=False, worker_id=None):
        self.headless = headless
        self.worker_id = worker_id
        self.success_count = 0
        self.failed_accounts = []
        self.captcha_accounts = []
        self.debug_mode = True

    def get_random_language(self):
        return random.choice(["en-US", "en", "fr", "es", "de", "ja"])

    def get_random_vendor(self):
        return random.choice(["Google Inc.", "Mozilla Foundation", "Microsoft Corporation", "Apple Inc.", "Samsung Electronics", "IBM Corporation"])

    def get_random_platform(self):
        return random.choice(["Win32", "Linux x86_64", "Macintosh", "Android", "iOS", "Windows NT", "Ubuntu", "Fedora", "Chrome OS"])

    def get_random_webgl_vendor(self):
        return random.choice(["Intel Inc.", "NVIDIA Corporation", "AMD", "ARM", "Qualcomm", "Apple Inc.", "Broadcom", "Imagination Technologies"])

    def get_random_renderer(self):
        return random.choice(["Intel Iris OpenGL Engine", "NVIDIA GeForce GTX", "AMD Radeon", "ARM Mali", "Qualcomm Adreno", "Apple A12 Bionic"])

    def get_random_timezone(self):
        return random.choice(['US/Pacific', 'US/Mountain', 'US/Central', 'US/Eastern', 'Canada/Pacific', 'Canada/Mountain', 'Canada/Central', 'Canada/Eastern'])

    def get_random_window_size(self):
        return f"{random.randint(1024, 1920)}x{random.randint(768, 1080)}"

    def detect_captcha(self, driver):
        worker_prefix = f"[W{self.worker_id}] " if self.worker_id else ""
        try:
            captcha_selectors = [
                "iframe[src*='recaptcha']",
                "iframe[src*='captcha']", 
                ".g-recaptcha",
                ".recaptcha",
                "#captcha",
                "[data-captcha]",
                ".captcha-container",
                "div[data-callback*='captcha']"
            ]
            
            for selector in captcha_selectors:
                try:
                    if driver.find_elements(By.CSS_SELECTOR, selector):
                        print(f"{worker_prefix}🔍 CAPTCHA detected: {selector}")
                        return True
                except:
                    continue

            page_source = driver.page_source.lower()
            captcha_indicators = [
                "recaptcha-checkbox",
                "i'm not a robot", 
                "select all images",
                "please complete the security check",
                "solve this challenge",
                "verify you are human",
                "your computer or network may be sending automated queries"
            ]

            for indicator in captcha_indicators:
                if indicator in page_source:
                    print(f"{worker_prefix}🔍 CAPTCHA detected: {indicator}")
                    return True

            return False

        except Exception as e:
            print(f"{worker_prefix}❌ CAPTCHA detection error: {e}")
            return False

    def log_captcha_account(self, account, reason="CAPTCHA detected"):
        line = f"{account['email']}:{account['password']}"
        if account.get("totp_secret"):
            line += f":{account['totp_secret']}"
        line += f" | CAPTCHA: {reason} | {datetime.now().isoformat()}\n"

        with captcha_lock:
            with open("captcha.txt", "a") as f:
                f.write(line)
        
        self.captcha_accounts.append({"email": account["email"], "reason": reason})

    def load_gmail_accounts(self, file_path="gmail_accounts.txt"):
        accounts = []
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if ':' in line and '@' in line and not line.startswith('#'):
                        parts = line.split(':', 2)
                        if len(parts) >= 2:
                            accounts.append({
                                'email': parts[0].strip(),
                                'password': parts[1].strip(),
                                'totp_secret': parts[2].strip() if len(parts) > 2 else None,
                                'token_file': f"{parts[0].strip()}.json"
                            })
            return accounts
        except FileNotFoundError:
            return []

    def append_successful_account(self, account):
        line = f"{account['email']}:{account['password']}"
        if account.get('totp_secret'):
            line += f":{account['totp_secret']}"
        line += "\n"
        
        with file_lock:
            if not os.path.exists('success_accounts.txt'):
                with open('success_accounts.txt', 'w') as f:
                    f.write(line)
                return
            with open('success_accounts.txt', 'r+') as f:
                existing = [l.strip() for l in f.read().splitlines()]
                if line.strip() not in existing:
                    f.write(line)

    def append_failed_account(self, account, error_reason):
        line = f"{account['email']}:{account['password']}"
        if account.get('totp_secret'):
            line += f":{account['totp_secret']}"
        line += f" | ERROR: {error_reason}\n"

        with file_lock:
            with open("failed.txt", "a") as f:
                f.write(line)

    def create_stealth_driver(self):
        worker_prefix = f"[W{self.worker_id}] " if self.worker_id else ""
        chrome_options = webdriver.ChromeOptions()
        
        # IDENTICAL anti-detection flags for both headless and non-headless
        stealth_flags = [
            "--incognito",
            "--disable-plugins",
            "--disable-extensions-file-access-check", 
            "--disable-popup-blocking",
            "--disable-blink-features=AutomationControlled",
            "--disable-web-security",
            "--allow-running-insecure-content",
            "--ignore-certificate-errors",
            "--no-sandbox",
            "--disable-dev-shm-usage"
        ]
        
        for flag in stealth_flags:
            chrome_options.add_argument(flag)

        # MODE-SPECIFIC flags
        if self.headless:
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
            print(f"{worker_prefix}🕶️ Headless mode")
        else:
            chrome_options.add_argument('--start-maximized')
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36")

        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_experimental_option("prefs", {"javascript.enabled": True})

        # IDENTICAL prefs for both modes  
        prefs = {
            'profile.managed_default_content_settings.images': 1,  # ALLOW images (critical for CAPTCHA avoidance)
            'profile.managed_default_content_settings.video': 2,
            'profile.default_content_setting_values.notifications': 2,
            'credentials_enable_service': False,
            'profile.password_manager_enabled': False,
            'autofill.profile_enabled': False,
            'profile.pick_account_handler_behavior': 'skip'
        }
        chrome_options.add_experimental_option('prefs', prefs)
        
        chrome_options.add_argument('--window-size=' + self.get_random_window_size())
        chrome_options.add_argument('--force-timezone=' + self.get_random_timezone())

        driver = webdriver.Chrome(options=chrome_options)
        
        # IDENTICAL stealth injection for both modes
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', { get: () => false });
                Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
                Object.defineProperty(navigator, 'vendor', { get: () => 'Google Inc.' });
                Object.defineProperty(navigator, 'platform', { get: () => 'MacIntel' });
            """
        })
        
        stealth(driver,
            languages=[self.get_random_language()],
            vendor=self.get_random_vendor(),
            platform=self.get_random_platform(),
            webgl_vendor=self.get_random_webgl_vendor(),
            renderer=self.get_random_renderer()
        )
        
        return driver

    def build_oauth_url(self):
        import urllib.parse
        params = {
            'client_id': CLIENT_CONFIG['web']['client_id'],
            'redirect_uri': CLIENT_CONFIG['web']['redirect_uris'][0],
            'scope': ' '.join(SCOPES),
            'response_type': 'code',
            'access_type': 'offline',
            'prompt': 'consent',
            'state': f'state_{random.randint(1000000, 9999999)}'
        }
        return f"https://accounts.google.com/o/oauth2/v2/auth?{urllib.parse.urlencode(params)}"

    def save_debug_screenshot(self, driver, email, stage, error=""):
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            worker_suffix = f"_W{self.worker_id}" if self.worker_id else ""
            filename = f"debug_{email.replace('@', '_')}_{stage}_{timestamp}{worker_suffix}.png"
            
            os.makedirs("screenshots", exist_ok=True)
            screenshot_path = os.path.join("screenshots", filename)
            driver.save_screenshot(screenshot_path)
            
            metadata = {
                "email": email,
                "stage": stage,
                "error": error,
                "timestamp": timestamp,
                "worker_id": self.worker_id,
                "url": driver.current_url,
                "title": driver.title,
                "page_source_length": len(driver.page_source)
            }
            
            metadata_path = screenshot_path.replace(".png", "_info.json")
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2)
                
            worker_prefix = f"[W{self.worker_id}] " if self.worker_id else ""
            print(f"{worker_prefix}📸 Screenshot: {filename}")
            
        except Exception as e:
            print(f"Screenshot failed: {e}")

    def handle_unverified_app_warning(self, driver, wait):
        worker_prefix = f"[W{self.worker_id}] " if self.worker_id else ""
        print(f"{worker_prefix}🔍 Checking unverified app warning...")
        time.sleep(5)

        try:
            page_source = driver.page_source.lower()
            warning_indicators = [
                "google hasn't verified this app",
                "this app isn't verified",
                "unverified app",
                "go to app (unsafe)",
                "advanced",
                "لم تثبت شركة google ملكية هذا التطبيق",
                "يطلب التطبيق الإذن بالوصول إلى معلومات حسّاسة",
                "الخيارات المتقدمة",
                "إخفاء الخيارات المتقدمة",
                "الانتقال إلى",
                "غير آمن"
            ]
            
            if not any(ind in page_source for ind in warning_indicators):
                print(f"{worker_prefix}✅ No warning detected")
                return True

            print(f"{worker_prefix}⚠️ Warning detected - bypassing")

            # Click Advanced
            advanced_selectors = ['[jsname="BO4nrb"]', "a.xTI6Gf.vh6Iad"]
            for sel in advanced_selectors:
                clicked = driver.execute_script(f"""
                    const els = document.querySelectorAll('{sel}');
                    for (let el of els) {{
                        const t = (el.textContent||el.innerText||'').trim().toLowerCase();
                        if (t === 'advanced' || t === 'الخيارات المتقدمة' || t === 'إخفاء الخيارات المتقدمة') {{
                            el.click(); return true;
                        }}
                    }}
                    return false;
                """)
                if clicked:
                    print(f"{worker_prefix}✅ Advanced clicked")
                    time.sleep(2)
                    break
            else:
                return False

            time.sleep(2)
            
            # Click unsafe/go to app
            unsafe_selectors = ['[jsname="ehL7e"]', "a.xTI6Gf.vh6Iad"]
            for sel in unsafe_selectors:
                clicked = driver.execute_script(f"""
                    const els = document.querySelectorAll('{sel}');
                    for (let el of els) {{
                        const t = (el.textContent||el.innerText||'').trim().toLowerCase();
                        if (t.includes('go to') || t.includes('unsafe') || t.includes('الانتقال إلى') || t.includes('غير آمن')) {{
                            el.click(); return true;
                        }}
                    }}
                    return false;
                """)
                if clicked:
                    print(f"{worker_prefix}✅ Unsafe clicked")
                    time.sleep(2)
                    return True

            return False

        except Exception as e:
            print(f"{worker_prefix}❌ Warning bypass failed: {e}")
            return False

    def automate_oauth_flow(self, account):
        email = account["email"]
        password = account["password"]
        totp_secret = account.get("totp_secret")
        
        worker_prefix = f"[W{self.worker_id}] " if self.worker_id else ""
        print(f"{worker_prefix}🔐 OAuth flow: {email}")

        driver = self.create_stealth_driver()
        wait = WebDriverWait(driver, 20)

        try:
            # Initial page load
            driver.get('https://devrahman.com/')
            driver.implicitly_wait(time_to_wait=10)
            time.sleep(1)

            # Check for CAPTCHA after initial load
            if self.detect_captcha(driver):
                self.log_captcha_account(account, "CAPTCHA on initial site")
                self.save_debug_screenshot(driver, email, "captcha_initial")
                return None

            # Multi-tab OAuth approach
            oauth_url = self.build_oauth_url()
            driver.execute_script(f'''window.open("{oauth_url}","_blank");''')
            driver.switch_to.window(driver.window_handles[0])
            driver.close()
            driver.implicitly_wait(time_to_wait=10)
            driver.switch_to.window(driver.window_handles[0])
            driver.implicitly_wait(time_to_wait=10)
            time.sleep(1)

            # Check for CAPTCHA after OAuth navigation
            if self.detect_captcha(driver):
                self.log_captcha_account(account, "CAPTCHA on OAuth page")
                self.save_debug_screenshot(driver, email, "captcha_oauth")
                return None

            # Enter email
            driver.find_element(By.ID, 'identifierId').send_keys(email)
            driver.find_element(By.ID, 'identifierNext').click()
            driver.implicitly_wait(time_to_wait=10)

            # Wait for password field
            element = wait.until(EC.presence_of_element_located((By.NAME, "Passwd")))
            time.sleep(2)

            # Check for CAPTCHA after email entry
            if self.detect_captcha(driver):
                self.log_captcha_account(account, "CAPTCHA after email entry")
                self.save_debug_screenshot(driver, email, "captcha_after_email")
                return None

            # Enter password
            driver.find_element(By.NAME, 'Passwd').send_keys(password)
            driver.find_element(By.ID, 'passwordNext').click()
            driver.implicitly_wait(time_to_wait=10)

            # Check for CAPTCHA after password entry
            if self.detect_captcha(driver):
                self.log_captcha_account(account, "CAPTCHA after password entry")
                self.save_debug_screenshot(driver, email, "captcha_after_password")
                return None

            # Handle 2FA if provided
            if totp_secret:
                self.handle_2fa(driver, wait, totp_secret)

            # Handle unverified app warning
            self.handle_unverified_app_warning(driver, wait)

            # Handle consent screen
            self.handle_consent_screen(driver, wait)

            # Final CAPTCHA check
            if self.detect_captcha(driver):
                self.log_captcha_account(account, "CAPTCHA during consent/warning")
                self.save_debug_screenshot(driver, email, "captcha_final")
                return None

            # Wait for redirect
            return self.wait_for_redirect(driver, wait)

        except Exception as e:
            # Check for CAPTCHA on error
            if self.detect_captcha(driver):
                self.log_captcha_account(account, f"CAPTCHA on error: {str(e)}")
                self.save_debug_screenshot(driver, email, "captcha_error")
                return None
                
            if self.debug_mode:
                self.save_debug_screenshot(driver, email, "error", str(e))

            print(f"{worker_prefix}❌ Failed: {str(e)}")
            return None

        finally:
            driver.quit()

    def handle_2fa(self, driver, wait, totp_secret):
        worker_prefix = f"[W{self.worker_id}] " if self.worker_id else ""
        try:
            import pyotp
            totp_selectors = ['input[type="tel"]', 'input[name="totpPin"]', 'input[aria-label*="code"]']

            for selector in totp_selectors:
                try:
                    totp_input = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                    if totp_input:
                        totp = pyotp.TOTP(totp_secret)
                        current_code = totp.now()
                        print(f"{worker_prefix}🔐 2FA code: {current_code}")
                        totp_input.send_keys(current_code)
                        submit_btn = driver.find_element(By.ID, 'totpNext')
                        submit_btn.click()
                        time.sleep(3)
                        print(f"{worker_prefix}✅ 2FA submitted")
                        break
                except:
                    continue
        except Exception as e:
            print(f"{worker_prefix}❌ 2FA failed: {str(e)}")

    def handle_consent_screen(self, driver, wait):
        worker_prefix = f"[W{self.worker_id}] " if self.worker_id else ""
        print(f"{worker_prefix}🔍 Consent screen...")
        time.sleep(2)

        consent_selectors = [
            "//span[contains(text(), 'Continue')]",
            "//span[contains(text(), 'Allow')]",
            "//span[contains(text(), 'متابعة')]",
            "//button[@jsname='LgbsSe']",
            "//button[@id='submit_approve_access']",
            "//input[@value='Allow']"
        ]

        for selector in consent_selectors:
            try:
                btn = wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
                btn.click()
                print(f"{worker_prefix}✅ Consent clicked")
                time.sleep(2)
                return True
            except:
                continue

        print(f"{worker_prefix}❌ No consent button")
        return True

    def wait_for_redirect(self, driver, wait):
        worker_prefix = f"[W{self.worker_id}] " if self.worker_id else ""
        try:
            def check_redirect(driver):
                url = driver.current_url
                return "code=" in url or "localhost" in url or "error=" in url

            wait.until(check_redirect)
            current_url = driver.current_url
            print(f"{worker_prefix}📍 Final URL: {current_url}")

            if "error=" in current_url:
                error = parse_qs(urlparse(current_url).query).get('error', ['Unknown'])[0]
                raise Exception(f"OAuth error: {error}")

            parsed_url = urlparse(current_url)
            auth_code = parse_qs(parsed_url.query).get('code', [None])[0]

            if auth_code:
                print(f"{worker_prefix}✅ Auth code obtained")
                return auth_code
            else:
                raise Exception("No auth code found")

        except Exception as e:
            print(f"{worker_prefix}❌ Redirect failed: {str(e)}")
            return None

    def exchange_code_for_tokens(self, auth_code, email):
        worker_prefix = f"[W{self.worker_id}] " if self.worker_id else ""
        data = {
            'client_id': CLIENT_CONFIG['web']['client_id'],
            'client_secret': CLIENT_CONFIG['web']['client_secret'],
            'code': auth_code,
            'grant_type': 'authorization_code',
            'redirect_uri': CLIENT_CONFIG['web']['redirect_uris'][0]
        }

        response = requests.post(CLIENT_CONFIG['web']['token_uri'], data=data)

        if response.status_code == 200:
            token_data = response.json()
            creds = Credentials(
                token=token_data['access_token'],
                refresh_token=token_data.get('refresh_token'),
                token_uri=CLIENT_CONFIG['web']['token_uri'],
                client_id=CLIENT_CONFIG['web']['client_id'],
                client_secret=CLIENT_CONFIG['web']['client_secret'],
                scopes=SCOPES
            )
            return creds
        else:
            raise Exception(f"Token exchange failed: {response.text}")

    def save_token(self, credentials, token_file):
        with open(token_file, 'w') as f:
            f.write(credentials.to_json())

    def process_account(self, account):
        worker_prefix = f"[W{self.worker_id}] " if self.worker_id else ""
        email = account['email']
        
        print(f"{worker_prefix}🔄 Processing {email}")

        try:
            auth_code = self.automate_oauth_flow(account)
            if auth_code:
                credentials = self.exchange_code_for_tokens(auth_code, email)
                self.append_successful_account(account)
                self.save_token(credentials, account['token_file'])
                self.success_count += 1
                print(f"{worker_prefix}🎉 Success: {email}")
                return True
            else:
                error_reason = "OAuth flow failed or CAPTCHA detected"
                self.failed_accounts.append({'email': email, 'error': error_reason})
                self.append_failed_account(account, error_reason)
                return False
        except Exception as e:
            error_reason = str(e)
            print(f"{worker_prefix}❌ Failed {email}: {error_reason}")
            self.failed_accounts.append({'email': email, 'error': error_reason})
            self.append_failed_account(account, error_reason)
            return False

    def process_all_accounts(self):
        accounts = self.load_gmail_accounts()
        if not accounts:
            print("❌ No accounts found")
            return

        worker_prefix = f"[W{self.worker_id}] " if self.worker_id else ""
        print(f"{worker_prefix}🚀 Processing {len(accounts)} accounts")
        if self.headless:
            print(f"{worker_prefix}🕶️ Headless mode")

        # Clear previous logs
        for log_file in ["failed.txt", "captcha.txt"]:
            if os.path.exists(log_file):
                os.remove(log_file)

        for account in accounts:
            self.process_account(account)
            time.sleep(random.uniform(1, 3))

        print(f"{worker_prefix}📊 Success: {self.success_count}, Failed: {len(self.failed_accounts)}, CAPTCHA: {len(self.captcha_accounts)}")

def process_accounts_worker(accounts_chunk, worker_id, headless):
    automator = GmailOAuth2Automator(headless=headless, worker_id=worker_id)
    
    for account in accounts_chunk:
        automator.process_account(account)
        time.sleep(random.uniform(1, 3))
    
    return {
        "worker_id": worker_id,
        "success_count": automator.success_count,
        "failed_count": len(automator.failed_accounts),
        "captcha_count": len(automator.captcha_accounts)
    }

def create_sample_accounts_file():
    if not os.path.exists('gmail_accounts.txt'):
        sample_content = """# Gmail OAuth2 Automation
# Format: email:password or email:password:totp_secret
# Examples:
# user1@gmail.com:password123
# user2@gmail.com:password456:JBSWY3DPEHPK3PXP

# Add your accounts below:
# youremail@gmail.com:yourpassword
"""
        with open('gmail_accounts.txt', 'w') as f:
            f.write(sample_content)
        print("📝 Created gmail_accounts.txt")
        return False
    return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    parser.add_argument("--workers", type=int, default=1, help="Number of workers")
    args = parser.parse_args()

    print("🚀 Gmail OAuth2 Production Automator")
    print("Identical stealth flags for headless and non-headless modes")
    print("=" * 60)

    if not create_sample_accounts_file():
        print("✏️ Edit gmail_accounts.txt and add credentials")
        return

    automator = GmailOAuth2Automator()
    accounts = automator.load_gmail_accounts()

    if not accounts:
        print("❌ No valid accounts found")
        return

    print(f"📧 Found {len(accounts)} accounts")
    if args.headless:
        print("🕶️ Headless mode enabled")
    if args.workers > 1:
        print(f"👥 Using {args.workers} workers")

    response = input("Proceed? (y/n): ")
    if response.lower() != 'y':
        print("❌ Cancelled")
        return

    if args.workers == 1:
        automator_single = GmailOAuth2Automator(headless=args.headless)
        automator_single.process_all_accounts()
    else:
        chunk_size = max(1, len(accounts) // args.workers)
        account_chunks = [accounts[i:i + chunk_size] for i in range(0, len(accounts), chunk_size)]
        
        total_success = 0
        total_failed = 0
        total_captcha = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_worker = {
                executor.submit(process_accounts_worker, chunk, i + 1, args.headless): i + 1
                for i, chunk in enumerate(account_chunks)
            }
            
            for future in concurrent.futures.as_completed(future_to_worker):
                worker_id = future_to_worker[future]
                try:
                    result = future.result()
                    total_success += result["success_count"]
                    total_failed += result["failed_count"] 
                    total_captcha += result["captcha_count"]
                    print(f"✅ Worker {worker_id}: {result['success_count']} success, {result['failed_count']} failed, {result['captcha_count']} captcha")
                except Exception as exc:
                    print(f"❌ Worker {worker_id} exception: {exc}")
        
        print(f"\n📊 FINAL RESULTS")
        print(f"✅ Total Success: {total_success}")
        print(f"❌ Total Failed: {total_failed}")
        print(f"🔍 Total CAPTCHA: {total_captcha}")

    print("\n🎉 Automation completed!")
    print("💾 Files: success_accounts.txt, failed.txt, captcha.txt, *.json tokens")
    print("📸 Screenshots: ./screenshots/")

if __name__ == "__main__":
    main()
