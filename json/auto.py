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
from selenium.common.exceptions import TimeoutException, NoSuchElementException
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
                            
                            accounts.append({
                                'email': email,
                                'password': password,
                                'totp_secret': totp_secret,
                                'token_file': f'token_{line_num}.json'
                            })
            return accounts
        except FileNotFoundError:
            print("❌ gmail_accounts.txt not found")
            return []
    
    def create_stealth_driver(self):
        """Create stealth Chrome driver with anti-detection capabilities"""
        chrome_options = webdriver.ChromeOptions()
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
    
    def detect_captcha_presence(self, driver):
        """Detect if CAPTCHA is present on the current page"""
        try:
            time.sleep(2)  # Allow page to load
            
            page_source = driver.page_source.lower()
            captcha_indicators = [
                "captcha",
                "recaptcha", 
                "confirm you're not a robot",
                "verify it's you",
                "i'm not a robot",
                "security check",
                "suspicious activity detected",
                "verify that you're human",
                "please complete the security check"
            ]
            
            # Check page source for CAPTCHA indicators
            text_captcha_present = any(indicator in page_source for indicator in captcha_indicators)
            
            # Check for CAPTCHA iframe elements
            iframe_captcha_selectors = [
                "iframe[src*='recaptcha']",
                "iframe[src*='captcha']",
                "iframe[title*='recaptcha']",
                "iframe[title*='captcha']"
            ]
            
            iframe_captcha_present = False
            for selector in iframe_captcha_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements and any(elem.is_displayed() for elem in elements):
                        iframe_captcha_present = True
                        break
                except:
                    continue
            
            # Check for CAPTCHA div elements
            div_captcha_selectors = [
                "div[class*='recaptcha']",
                "div[class*='captcha']",
                "div[id*='recaptcha']",
                "div[id*='captcha']"
            ]
            
            div_captcha_present = False
            for selector in div_captcha_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements and any(elem.is_displayed() for elem in elements):
                        div_captcha_present = True
                        break
                except:
                    continue
            
            captcha_detected = text_captcha_present or iframe_captcha_present or div_captcha_present
            
            if captcha_detected:
                print("🤖 CAPTCHA detected on page")
            
            return captcha_detected
            
        except Exception as e:
            print(f"⚠️ Error detecting CAPTCHA: {str(e)}")
            return False
    
    def wait_for_captcha_completion_with_element_detection(self, driver, wait):
        """Enhanced CAPTCHA detection using element state monitoring"""
        try:
            print("🔍 Monitoring for CAPTCHA completion via element detection...")
            
            # First, detect if CAPTCHA is present
            captcha_present = self.detect_captcha_presence(driver)
            
            if not captcha_present:
                print("✅ No CAPTCHA detected, proceeding...")
                return True
            
            print("⏳ CAPTCHA detected - waiting for manual completion...")
            print("👆 Please solve the CAPTCHA manually using mouse/touchpad")
            print("🔄 Automation will resume automatically when CAPTCHA is solved...")
            
            # Multiple detection strategies for CAPTCHA completion
            success_indicators = [
                # Elements that appear after successful login/verification
                (By.XPATH, "//div[contains(@class, 'success')]"),
                (By.XPATH, "//h1[contains(text(), 'Allow')]"),
                (By.XPATH, "//span[contains(text(), 'Continue')]"),
                (By.XPATH, "//button[contains(text(), 'Allow')]"),
                (By.XPATH, "//button[contains(text(), 'Continue')]"),
                (By.ID, "submit_approve_access"),
                (By.XPATH, "//input[@value='Allow']"),
                (By.XPATH, "//span[text()='Next']"),
                (By.ID, "identifierNext"),
                (By.ID, "passwordNext"),
                (By.XPATH, "//div[contains(@class, 'oauth')]"),
                (By.XPATH, "//h1[text()='Choose an account']")
            ]
            
            # Wait for any success indicator to appear (extended timeout for manual solving)
            for locator_type, locator_value in success_indicators:
                try:
                    element = WebDriverWait(driver, 5).until(
                        EC.any_of(
                            EC.element_to_be_clickable((locator_type, locator_value)),
                            EC.presence_of_element_located((locator_type, locator_value))
                        )
                    )
                    if element and element.is_displayed():
                        print(f"✅ CAPTCHA completion detected via element: {locator_value}")
                        time.sleep(2)  # Brief pause for page stabilization
                        return True
                except TimeoutException:
                    continue
                except Exception as e:
                    continue
            
            # Alternative: Monitor for URL changes indicating progress
            original_url = driver.current_url
            try:
                print("🔄 Monitoring URL changes for CAPTCHA completion...")
                WebDriverWait(driver, 90).until(
                    lambda driver: driver.current_url != original_url
                )
                print("✅ CAPTCHA completion detected via URL change")
                time.sleep(2)
                return True
            except TimeoutException:
                print("⏰ URL change timeout, trying alternative detection...")
            
            # Fallback: Monitor for CAPTCHA elements to disappear
            return self.monitor_captcha_disappearance(driver, wait)
            
        except Exception as e:
            print(f"❌ Element detection error: {str(e)}")
            return False

    def monitor_captcha_disappearance(self, driver, wait):
        """Monitor for CAPTCHA elements to disappear indicating completion"""
        try:
            print("🔍 Monitoring CAPTCHA element disappearance...")
            
            # Wait for CAPTCHA elements to become invisible
            captcha_disappear_conditions = [
                EC.invisibility_of_element_located((By.XPATH, "//iframe[contains(@src, 'recaptcha')]")),
                EC.invisibility_of_element_located((By.XPATH, "//*[contains(text(), 'not a robot')]")),
                EC.invisibility_of_element_located((By.XPATH, "//*[contains(text(), 'verify')]")),
                EC.invisibility_of_element_located((By.CSS_SELECTOR, "div[class*='recaptcha']"))
            ]
            
            try:
                WebDriverWait(driver, 60).until(
                    EC.any_of(*captcha_disappear_conditions)
                )
                print("✅ CAPTCHA elements disappeared - assuming completion")
                time.sleep(3)
                return True
            except TimeoutException:
                print("⏰ CAPTCHA disappearance timeout")
                return False
                
        except Exception as e:
            print(f"⚠️ CAPTCHA disappearance monitoring failed: {str(e)}")
            return False

    def comprehensive_captcha_completion_detection(self, driver, wait):
        """Comprehensive CAPTCHA completion detection using multiple strategies"""
        print("🔍 Starting comprehensive CAPTCHA completion detection...")
        
        # Strategy 1: Wait for success elements to appear
        success_conditions = [
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'oauth-success')]")),
            EC.presence_of_element_located((By.XPATH, "//h1[text()='Choose an account']")),
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Continue')]")),
            EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'Allow')]")),
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'consent')]"))
        ]
        
        # Strategy 2: Wait for CAPTCHA elements to disappear
        captcha_disappear_conditions = [
            EC.invisibility_of_element_located((By.XPATH, "//iframe[contains(@src, 'recaptcha')]")),
            EC.invisibility_of_element_located((By.XPATH, "//*[contains(text(), 'not a robot')]")),
            EC.invisibility_of_element_located((By.XPATH, "//*[contains(text(), 'verify')]"))
        ]
        
        try:
            # Wait for either success indicators OR CAPTCHA disappearance
            WebDriverWait(driver, 120).until(  # 2 minutes timeout for manual solving
                EC.any_of(
                    *success_conditions,
                    *captcha_disappear_conditions
                )
            )
            
            print("✅ CAPTCHA completion detected through comprehensive monitoring")
            time.sleep(3)  # Brief pause for page stabilization
            return True
            
        except TimeoutException:
            print("⏰ Comprehensive CAPTCHA detection timeout - proceeding anyway")
            return True  # Continue process even if detection fails
        except Exception as e:
            print(f"❌ Comprehensive detection error: {str(e)}")
            return True  # Continue process even if detection fails
    
    def handle_unverified_app_warning(self, driver, wait):
        """Enhanced method to handle unverified app warning screen"""
        print("🔍 Checking for unverified app warning...")
        time.sleep(5)  # Increased wait time
        
        try:
            # Check if warning page is present
            page_source = driver.page_source.lower()
            warning_indicators = [
                "google hasn't verified this app",
                "this app isn't verified",
                "unverified app",
                "go to app (unsafe)",
                "advanced"
            ]
            
            warning_present = any(indicator in page_source for indicator in warning_indicators)
            
            if not warning_present:
                print("✅ No unverified app warning detected")
                return True
            
            print("⚠️ Unverified app warning detected - attempting bypass")
            
            # Multiple selector strategies for Advanced button
            advanced_selectors = [
                '[jsname="BO4nrb"]',  # Original selector
                'button[data-value="advanced"]',
                'span:contains("Advanced")',
                '[role="button"]:contains("Advanced")',
                'div[data-value="advanced"]',
                'button:contains("Advanced")',
                '[aria-label*="Advanced"]'
            ]
            
            # Try JavaScript click on Advanced button
            for selector in advanced_selectors:
                advanced_js = f"""
                var elements = document.querySelectorAll('{selector}');
                for (var i = 0; i < elements.length; i++) {{
                    if (elements[i].textContent.toLowerCase().includes('advanced') || 
                        elements[i].innerText.toLowerCase().includes('advanced')) {{
                        elements[i].click();
                        return true;
                    }}
                }}
                return false;
                """
                
                if driver.execute_script(advanced_js):
                    print(f"✅ Clicked Advanced button using selector: {selector}")
                    time.sleep(3)
                    break
            else:
                # Fallback: Try to find Advanced by text content
                try:
                    advanced_elements = driver.find_elements(By.XPATH, "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'advanced')]")
                    for element in advanced_elements:
                        if element.is_displayed() and element.is_enabled():
                            driver.execute_script("arguments[0].click();", element)
                            print("✅ Clicked Advanced button using text fallback")
                            time.sleep(3)
                            break
                except Exception as e:
                    print(f"⚠️ Advanced button fallback failed: {e}")
                    return False
            
            # Wait for unsafe button to appear and try multiple selectors
            time.sleep(3)
            
            unsafe_selectors = [
                '[jsname="ehL7e"]',  # Original selector
                'button:contains("unsafe")',
                'span:contains("unsafe")',
                '[role="button"]:contains("Go to")',
                'button[data-value="unsafe"]',
                '[aria-label*="unsafe"]'
            ]
            
            # Try JavaScript click on unsafe button
            for selector in unsafe_selectors:
                unsafe_js = f"""
                var elements = document.querySelectorAll('{selector}');
                for (var i = 0; i < elements.length; i++) {{
                    var text = (elements[i].textContent || elements[i].innerText).toLowerCase();
                    if (text.includes('unsafe') || text.includes('go to')) {{
                        elements[i].click();
                        return true;
                    }}
                }}
                return false;
                """
                
                if driver.execute_script(unsafe_js):
                    print(f"✅ Clicked unsafe button using selector: {selector}")
                    time.sleep(3)
                    return True
            
            # Final fallback: Look for any clickable element with "unsafe" or "continue"
            try:
                unsafe_elements = driver.find_elements(By.XPATH, "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'unsafe') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'continue') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'go to')]")
                
                for element in unsafe_elements:
                    try:
                        if element.is_displayed() and element.is_enabled():
                            driver.execute_script("arguments[0].click();", element)
                            print(f"✅ Clicked unsafe button using text: {element.text}")
                            time.sleep(3)
                            return True
                    except Exception as e:
                        continue
                        
            except Exception as e:
                print(f"❌ All bypass attempts failed: {e}")
                return False
                
            return False
            
        except Exception as e:
            print(f"❌ Error handling unverified app warning: {str(e)}")
            return False

    def automate_oauth_flow(self, email, password, totp_secret=None):
        """Enhanced OAuth2 automation flow with comprehensive CAPTCHA handling"""
        print(f"🔐 Starting OAuth flow for {email}")
        
        driver = self.create_stealth_driver()
        wait = WebDriverWait(driver, 120)  # Extended timeout for manual CAPTCHA solving
        
        try:
            # Use the multi-tab approach for better stealth
            driver.get('https://github.com/')  # Start with non-Google site
            time.sleep(2)
            
            # Open OAuth URL in new tab
            oauth_url = self.build_oauth_url()
            driver.execute_script(f'''window.open("{oauth_url}","_blank");''')
            driver.switch_to.window(driver.window_handles[0])
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
            time.sleep(2)
            
            # Enter email
            email_field = wait.until(EC.presence_of_element_located((By.ID, 'identifierId')))
            email_field.send_keys(email)
            driver.find_element(By.ID, 'identifierNext').click()
            time.sleep(3)
            
            # Enhanced CAPTCHA detection after email entry
            self.wait_for_captcha_completion_with_element_detection(driver, wait)
            
            # Wait for password field and enter password
            password_field = wait.until(EC.presence_of_element_located((By.NAME, "Passwd")))
            time.sleep(2)
            
            password_field.send_keys(password)
            driver.find_element(By.ID, 'passwordNext').click()
            time.sleep(3)
            
            # Enhanced CAPTCHA detection after password entry
            self.wait_for_captcha_completion_with_element_detection(driver, wait)
            
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
            # Don't quit driver immediately if manual intervention might be needed
            if not self.debug_mode:
                driver.quit()
            else:
                print("🔧 Debug mode: Browser kept open for inspection")
                time.sleep(5)
                driver.quit()
    
    def handle_2fa(self, driver, wait, totp_secret):
        """Handle 2FA authentication"""
        try:
            import pyotp
            
            print("🔐 Handling 2FA authentication...")
            
            # Look for 2FA input with multiple selectors
            totp_selectors = [
                'input[type="tel"]',
                'input[name="totpPin"]',
                'input[aria-label*="code"]',
                'input[placeholder*="code"]',
                'input[id*="totp"]'
            ]
            
            for selector in totp_selectors:
                try:
                    totp_input = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                    if totp_input and totp_input.is_displayed():
                        # Generate TOTP code
                        totp = pyotp.TOTP(totp_secret)
                        current_code = totp.now()
                        
                        print(f"🔑 Generated 2FA code: {current_code}")
                        totp_input.clear()
                        totp_input.send_keys(current_code)
                        
                        # Submit 2FA
                        submit_selectors = ['#totpNext', 'button[type="submit"]', 'input[type="submit"]']
                        for submit_selector in submit_selectors:
                            try:
                                submit_btn = driver.find_element(By.CSS_SELECTOR, submit_selector)
                                if submit_btn.is_displayed() and submit_btn.is_enabled():
                                    submit_btn.click()
                                    print("✅ 2FA code submitted")
                                    time.sleep(3)
                                    return True
                            except:
                                continue
                        break
                except TimeoutException:
                    continue
                except Exception as e:
                    continue
                    
            print("⚠️ 2FA input not found or submission failed")
            return False
                    
        except Exception as e:
            print(f"❌ 2FA handling failed: {str(e)}")
            return False
    
    def handle_consent_screen(self, driver, wait):
        """Handle OAuth consent screen"""
        try:
            print("🔍 Checking for consent screen...")
            time.sleep(3)
            
            # Multiple consent button selectors
            consent_selectors = [
                "//span[contains(text(), 'Continue')]",
                "//span[contains(text(), 'Allow')]",
                "//button[@id='submit_approve_access']",
                "//input[@value='Allow']",
                "//button[contains(text(), 'Allow')]",
                "//button[contains(text(), 'Continue')]"
            ]
            
            for selector in consent_selectors:
                try:
                    consent_btn = wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
                    if consent_btn and consent_btn.is_displayed():
                        consent_btn.click()
                        print("✅ Consent granted")
                        time.sleep(3)
                        return True
                except TimeoutException:
                    continue
                except Exception as e:
                    continue
                    
            # Fallback: Look for any button that might be consent-related
            try:
                all_buttons = driver.find_elements(By.TAG_NAME, "button")
                for button in all_buttons:
                    button_text = button.text.lower()
                    if any(word in button_text for word in ['allow', 'continue', 'accept', 'grant']):
                        if button.is_displayed() and button.is_enabled():
                            button.click()
                            print(f"✅ Consent granted via fallback: {button.text}")
                            time.sleep(3)
                            return True
            except Exception as e:
                print(f"⚠️ Consent fallback failed: {e}")
                    
        except Exception as e:
            print(f"⚠️ Consent handling error: {str(e)}")
        
        return True  # Continue even if consent handling fails
    
    def wait_for_redirect(self, driver, wait):
        """Wait for OAuth redirect and extract auth code"""
        try:
            print("⏳ Waiting for OAuth redirect...")
            
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
        print("🤖 Enhanced with automatic CAPTCHA detection and manual solving support")
        
        for i, account in enumerate(accounts):
            self.process_account(account)
            
            # Delay between accounts
       #     if i < len(accounts) - 1:
      #          delay = random.uniform(30, 45)  # Increased delay for better stealth
      #          print(f"⏳ Waiting {delay:.1f} seconds before next account...")
       #         time.sleep(delay)
        
        # Print summary
        print(f"\n📊 AUTOMATION SUMMARY")
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
    print("🚀 Gmail OAuth2 Token Generator with Enhanced CAPTCHA Handling")
    print("Using advanced anti-detection techniques and manual CAPTCHA support")
    print("=" * 60)
    
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
    print("🤖 CAPTCHA handling: Automatic detection with manual solving support")
    print("🖱️  Mouse/touchpad interactions fully supported")
    
    response = input(f"Proceed with automation? (y/n): ")
    
    if response.lower() == 'y':
        automator.process_all_accounts()
        print("\n🎉 Automation completed!")
        print("💾 Token files are ready for your Gmail bulk sender")
    else:
        print("❌ Automation cancelled")

if __name__ == "__main__":
    main()
