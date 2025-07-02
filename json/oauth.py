#!/usr/bin/env python3

import time
import random
import os
import json
import requests
from datetime import datetime
from urllib.parse import parse_qs, urlparse
import subprocess
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium_stealth import stealth
from google.oauth2.credentials import Credentials

CLIENT_CONFIG = {"web":{"client_id":"560355320864-e2mt9vdkqck5r1956i9lcs2n8gc1u032.apps.googleusercontent.com","project_id":"fiery-webbing-463212-h0","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs","client_secret":"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","redirect_uris":["http://127.0.0.1:8080/","http://127.0.0.1:8080/oauth2callback","http://localhost:8080/","http://localhost:8080/oauth2callback"]}}


SCOPES = ['https://www.googleapis.com/auth/gmail.send']

class GmailOAuth2Automator:
    def __init__(self):
        self.success_count = 0
        self.failed_accounts = []
        self.debug_mode = True

    def get_random_language(self):
        languages = ["en-US", "en", "fr", "es", "de", "ja"]
        return random.choice(languages)

    def get_random_vendor(self):
        vendors = [
            "Google Inc.", "Mozilla Foundation", "Microsoft Corporation",
            "Apple Inc.", "Samsung Electronics", "IBM Corporation"
        ]
        return random.choice(vendors)

    def get_random_platform(self):
        platforms = [
            "Win32", "Linux x86_64", "Macintosh", "Android", "iOS",
            "Windows NT", "Ubuntu", "Fedora", "Chrome OS"
        ]
        return random.choice(platforms)

    def get_random_webgl_vendor(self):
        webgl_vendors = [
            "Intel Inc.", "NVIDIA Corporation", "AMD", "ARM", "Qualcomm",
            "Apple Inc.", "Broadcom", "Imagination Technologies"
        ]
        return random.choice(webgl_vendors)

    def get_random_renderer(self):
        renderers = [
            "Intel Iris OpenGL Engine", "NVIDIA GeForce GTX", "AMD Radeon",
            "ARM Mali", "Qualcomm Adreno", "Apple A12 Bionic"
        ]
        return random.choice(renderers)

    def get_random_timezone(self):
        timezones = ['US/Pacific', 'US/Mountain', 'US/Central', 'US/Eastern',
                    'Canada/Pacific', 'Canada/Mountain', 'Canada/Central', 'Canada/Eastern']
        return random.choice(timezones)

    def get_random_window_size(self):
        width = random.randint(1024, 1920)
        height = random.randint(768, 1080)
        return f"{width}x{height}"

    def load_gmail_accounts(self, file_path="gmail_accounts.txt"):
        """Load Gmail accounts from credentials file"""
        accounts = []
        try:
            with open(file_path, 'r') as f:
                for line_num, line in enumerate(f):
                    line = line.strip()
                    if ':' in line and '@' in line and not line.startswith('#'):
                        parts = line.split(':', 2)
                        if len(parts) >= 2:
                            email = parts[0].strip()
                            password = parts[1].strip()
                            totp_secret = parts[2].strip() if len(parts) > 2 else None
                            token_file = f"{email}.json"
                            accounts.append({
                                'email': email,
                                'password': password,
                                'totp_secret': totp_secret,
                                'token_file': token_file
                            })
            return accounts
        except FileNotFoundError:
            print("❌ gmail_accounts.txt not found")
            return []

    def append_successful_account(self, account):
        line = f"{account['email']}:{account['password']}"
        if account.get('totp_secret'):
            line += f":{account['totp_secret']}"
        line += "\n"
        if not os.path.exists('success_accounts.txt'):
            with open('success_accounts.txt', 'w') as f:
                f.write(line)
            return
        with open('success_accounts.txt', 'r+') as f:
            existing = [l.strip() for l in f.read().splitlines()]
            if line.strip() in existing:
                return
            f.write(line)

    def create_stealth_driver(self):
        """Create stealth Chrome driver with anti-detection capabilities"""
        chrome_options = webdriver.ChromeOptions()
        # chrome_options.add_argument("--user-data-dir=/Users/rahman/Library/Application Support/Google/Chrome")
        # chrome_options.add_argument("--profile-directory=Default")
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument('--start-maximized')
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-extensions-file-access-check")
        chrome_options.add_argument('--disable-popup-blocking')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--allow-running-insecure-content")
        chrome_options.add_argument("--ignore-certificate-errors")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])

        prefs = {
            'profile.managed_default_content_settings.images': 2,
            'profile.managed_default_content_settings.video': 2,
            'profile.default_content_setting_values.notifications': 2,
            'credentials_enable_service': False,
            'profile.password_manager_enabled': False,
            'autofill.profile_enabled': False,
            'profile.pick_account_handler_behavior': 'skip'
        }
        chrome_options.add_experimental_option('prefs', prefs)
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_experimental_option("prefs", {"javascript.enabled": True})
        chrome_options.add_argument('--window-size=' + self.get_random_window_size())
        chrome_options.add_argument('--force-timezone=' + self.get_random_timezone())

        driver = webdriver.Chrome(options=chrome_options)

        # Apply stealth with randomized parameters
        stealth(driver,
            languages=[self.get_random_language()],
            vendor=self.get_random_vendor(),
            platform=self.get_random_platform(),
            webgl_vendor=self.get_random_webgl_vendor(),
            renderer=self.get_random_renderer()
        )

        return driver

    def build_oauth_url(self):
        """Build OAuth2 authorization URL"""
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

    def handle_unverified_app_warning(self, driver, wait):
        """Detect and bypass the unverified-app warning screen in both English and Arabic."""
        print("🔍 Checking for unverified app warning...")
        time.sleep(5)  # Give the warning time to load

        try:
            # 1) Detect the warning page by English & Arabic phrases
            page_source = driver.page_source.lower()
            warning_indicators = [
                "google hasn't verified this app",
                "this app isn't verified",
                "unverified app",
                "go to app (unsafe)",
                "advanced",  # English “Advanced”
                "لم تثبت شركة google ملكية هذا التطبيق",  # Arabic header
                "يطلب التطبيق الإذن بالوصول إلى معلومات حسّاسة",  # Arabic description
                "الخيارات المتقدمة",  # Arabic “Advanced”
                "إخفاء الخيارات المتقدمة",  # Arabic “Hide advanced”
                "الانتقال إلى",  # Arabic “Go to …”
                "غير آمن",  # Arabic “unsafe”
            ]
            if not any(ind in page_source for ind in warning_indicators):
                print("✅ No unverified app warning detected")
                return True

            print("⚠️ Unverified app warning detected – attempting bypass")

            # 2) Click “Advanced” / “الخيارات المتقدمة”
            advanced_selectors = [
                '[jsname="BO4nrb"]',  # exact link for both languages
                "a.xTI6Gf.vh6Iad",  # fallback by Google’s link classes
            ]
            for sel in advanced_selectors:
                clicked = driver.execute_script(
                    f"""
                    const els = document.querySelectorAll('{sel}');
                    for (let el of els) {{
                        const t = (el.textContent||el.innerText||'').trim().toLowerCase();
                        if (t === 'advanced'
                        || t === 'الخيارات المتقدمة'
                        || t === 'إخفاء الخيارات المتقدمة') {{
                            el.click(); return true;
                        }}
                    }}
                    return false;
                """
                )
                if clicked:
                    print(f"✅ Clicked Advanced button using selector: {sel}")
                    time.sleep(2)
                    break
            else:
                print("❌ Failed to click Advanced; aborting.")
                return False

            # 3) Click “Go to GAM (unsafe)” / “الانتقال إلى GAM (غير آمن)”
            time.sleep(2)
            unsafe_selectors = [
                '[jsname="ehL7e"]',  # exact link for both languages
                "a.xTI6Gf.vh6Iad",  # fallback by Google’s link classes
            ]
            for sel in unsafe_selectors:
                clicked = driver.execute_script(
                    f"""
                    const els = document.querySelectorAll('{sel}');
                    for (let el of els) {{
                        const t = (el.textContent||el.innerText||'').trim().toLowerCase();
                        if (
                            t.includes('go to') ||
                            t.includes('unsafe') ||
                            t.includes('الانتقال إلى') ||
                            t.includes('غير آمن')
                        ) {{
                            el.click(); return true;
                        }}
                    }}
                    return false;
                """
                )
                if clicked:
                    print(f"✅ Clicked unsafe/app button using selector: {sel}")
                    time.sleep(2)
                    return True

            print("❌ All bypass attempts failed")
            return False

        except Exception as e:
            print(f"❌ Error handling unverified app warning: {e}")
            return False

    def automate_oauth_flow(self, email, password, totp_secret=None):
        """Main OAuth2 automation flow with comprehensive error handling"""
        print(f"🔐 Starting OAuth flow for {email}")

        driver = self.create_stealth_driver()
        wait = WebDriverWait(driver, 20)

        try:
            # Use the multi-tab approach for better stealth
            driver.get('https://devrahman.com/')  # Start with non-Google site
            driver.implicitly_wait(time_to_wait=10)
            time.sleep(1)

            # Open OAuth URL in new tab
            oauth_url = self.build_oauth_url()
            driver.execute_script(f'''window.open("{oauth_url}","_blank");''')
            driver.switch_to.window(driver.window_handles[0])
            driver.close()
            driver.implicitly_wait(time_to_wait=10)
            driver.switch_to.window(driver.window_handles[0])
            driver.implicitly_wait(time_to_wait=10)
            time.sleep(1)

            # Enter email
            driver.find_element(By.ID, 'identifierId').send_keys(email)
            driver.find_element(By.ID, 'identifierNext').click()
            driver.implicitly_wait(time_to_wait=10)

            # Wait for password field and enter password
            element = wait.until(EC.presence_of_element_located((By.NAME, "Passwd")))
            time.sleep(2)

            driver.find_element(By.NAME, 'Passwd').send_keys(password)
            driver.find_element(By.ID, 'passwordNext').click()
            driver.implicitly_wait(time_to_wait=10)

            # Handle 2FA if provided
            if totp_secret:
                self.handle_2fa(driver, wait, totp_secret)

            # Handle unverified app warning
            self.handle_unverified_app_warning(driver, wait)

            # Handle consent screen
            self.handle_consent_screen(driver, wait)

            # Wait for redirect and extract auth code
            return self.wait_for_redirect(driver, wait)

        except Exception as e:
            if self.debug_mode:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                screenshot_path = f"debug_{email.replace('@', '_')}_{timestamp}.png"
                driver.save_screenshot(screenshot_path)
                print(f"🖼️ Debug screenshot saved: {screenshot_path}")

            print(f"❌ OAuth flow failed for {email}: {str(e)}")
            return None

        finally:
            driver.quit()

    def handle_2fa(self, driver, wait, totp_secret):
        """Handle 2FA authentication"""
        try:
            import pyotp

            # Look for 2FA input
            totp_selectors = [
                'input[type="tel"]',
                'input[name="totpPin"]',
                'input[aria-label*="code"]'
            ]

            for selector in totp_selectors:
                try:
                    totp_input = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                    if totp_input:
                        # Generate TOTP code
                        totp = pyotp.TOTP(totp_secret)
                        current_code = totp.now()

                        totp_input.send_keys(current_code)

                        # Submit 2FA
                        submit_btn = driver.find_element(By.ID, 'totpNext')
                        submit_btn.click()
                        time.sleep(3)
                        break
                except:
                    continue

        except Exception as e:
            print(f"❌ 2FA handling failed: {str(e)}")


    def handle_consent_screen(self, driver, wait):
        """Handle OAuth consent screen (English & Arabic)"""
        print("🔍 Checking for consent screen...")
        time.sleep(2)

        # Add Arabic 'متابعة' and use jsname for robustness
        consent_selectors = [
            "//span[contains(text(), 'Continue')]",  # English Continue
            "//span[contains(text(), 'Allow')]",  # English Allow
            "//span[contains(text(), 'متابعة')]",  # Arabic Continue
            "//button[@jsname='LgbsSe']",  # Buttons with jsname LgbsSe
            "//button[@id='submit_approve_access']",  # Fallback by ID
            "//input[@value='Allow']",  # Input[Allow] fallback
        ]

        for selector in consent_selectors:
            try:
                btn = wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
                btn.click()
                print(f"✅ Clicked consent button using selector: {selector}")
                time.sleep(2)
                return True
            except:
                continue

        print("❌ Consent button not found; proceeding anyway")
        return True

    def wait_for_redirect(self, driver, wait):
        """Wait for OAuth redirect and extract auth code"""
        try:
            def check_redirect(driver):
                url = driver.current_url
                return "code=" in url or "localhost" in url or "error=" in url

            wait.until(check_redirect)

            current_url = driver.current_url
            print(f"📍 Final URL: {current_url}")

            if "error=" in current_url:
                error = parse_qs(urlparse(current_url).query).get('error', ['Unknown'])[0]
                raise Exception(f"OAuth error: {error}")

            # Extract authorization code
            parsed_url = urlparse(current_url)
            auth_code = parse_qs(parsed_url.query).get('code', [None])[0]

            if auth_code:
                print("✅ Authorization code obtained")
                return auth_code
            else:
                raise Exception("No authorization code found")

        except Exception as e:
            print(f"❌ Redirect handling failed: {str(e)}")
            return None

    def exchange_code_for_tokens(self, auth_code, email):
        """Exchange authorization code for tokens"""
        print(f"🔄 Exchanging code for tokens for {email}")

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
        """Save OAuth2 tokens to JSON file"""
        with open(token_file, 'w') as f:
            f.write(credentials.to_json())
        print(f"💾 Token saved to {token_file}")

    def process_account(self, account):
        """Process single account"""
        email = account['email']
        password = account['password']
        totp_secret = account.get('totp_secret')

        print(f"\n🔄 Processing {email}")

        try:
            auth_code = self.automate_oauth_flow(email, password, totp_secret)

            if auth_code:
                credentials = self.exchange_code_for_tokens(auth_code, email)
                self.append_successful_account(account)
                self.save_token(credentials, account['token_file'])
                self.success_count += 1
                print(f"🎉 Success for {email}")
                return True
            else:
                self.failed_accounts.append({'email': email, 'error': 'OAuth flow failed'})
                return False

        except Exception as e:
            print(f"❌ Failed {email}: {str(e)}")
            self.failed_accounts.append({'email': email, 'error': str(e)})
            return False

    def process_all_accounts(self):
        """Process all accounts from gmail_accounts.txt"""
        accounts = self.load_gmail_accounts()

        if not accounts:
            print("❌ No accounts found in gmail_accounts.txt")
            return

        print(f"🚀 Starting OAuth2 automation for {len(accounts)} accounts")

        for i, account in enumerate(accounts):
            self.process_account(account)

            # Delay between accounts
            # if i < len(accounts) - 1:
            #     delay = random.uniform(20, 30)
            #     print(f"⏳ Waiting {delay:.1f} seconds before next account...")
            #     time.sleep(delay)

        # Print summary
        print(f"\n📊 SUMMARY")
        print(f"✅ Successful: {self.success_count}")
        print(f"❌ Failed: {len(self.failed_accounts)}")

        if self.failed_accounts:
            print(f"\n❌ Failed Accounts:")
            for failed in self.failed_accounts:
                print(f"   • {failed['email']}: {failed['error']}")


def create_sample_accounts_file():
    """Create sample gmail_accounts.txt file"""
    if not os.path.exists('gmail_accounts.txt'):
        sample_content = """# Gmail OAuth2 Automation - Account Configuration
# Format: email:password or email:password:totp_secret
# Examples:
# user1@gmail.com:password123
# user2@gmail.com:password456:JBSWY3DPEHPK3PXP

# Add your accounts below:
# youremail@gmail.com:yourpassword
"""
        with open('gmail_accounts.txt', 'w') as f:
            f.write(sample_content)
        print("📝 Created gmail_accounts.txt template")
        return False
    return True

def main():
    """Main execution function"""
    print("🚀 Gmail OAuth2 Token Generator")
    print("Using advanced anti-detection techniques for Google bypass")
    print("=" * 50)
    
    # Check/create accounts file
    if not create_sample_accounts_file():
        print("✏️ Please edit gmail_accounts.txt and add your credentials")
        return
    
    # Create automator and run
    automator = GmailOAuth2Automator()
    accounts = automator.load_gmail_accounts()
    
    if not accounts:
        print("❌ No valid accounts found")
        return
    
    print(f"📧 Found {len(accounts)} accounts")
    response = input(f"Proceed with automation? (y/n): ")
    
    if response.lower() == 'y':
        automator.process_all_accounts()
        print("\n🎉 Automation completed!")
        print("💾 Token files are ready for your Gmail bulk sender")
    else:
        print("❌ Automation cancelled")

if __name__ == "__main__":
    main()
