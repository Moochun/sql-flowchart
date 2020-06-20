# Generated by Selenium IDE
import pytest
import time
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

class Test12356():
  def setup_method(self, method):
    self.driver = webdriver.Chrome()
    self.vars = {}
  
  def teardown_method(self, method):
    self.driver.quit()
  
  def test_12356(self):
    self.driver.get("https://mermaid-js.github.io/mermaid-live-editor/")
    self.driver.set_window_size(683, 728)
    self.driver.find_element(By.CSS_SELECTOR, ".view-line:nth-child(7)").click()
    self.driver.execute_script("window.scrollTo(0,0)")
    self.driver.execute_script("window.scrollTo(0,0)")
    self.driver.execute_script("window.scrollTo(0,0)")
    self.driver.execute_script("window.scrollTo(0,0)")
    self.driver.find_element(By.CSS_SELECTOR, ".focused .inputarea").send_keys("graph TD\\n  A[Christmas] -->|Get money| B(Go shopping)\\n  B --> C{Let me think}\\n  C -->|One| D[Laptop]\\n  C -->|Two| E[iPhone]\\n  C -->|Three| F[fa:fa-car Car]\\n		444\\n    555\\n    ")
  