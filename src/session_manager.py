import agentql
from agentql.ext.playwright import PlaywrightWebDriverSync

class SessionManager:
    def __init__(self, url):
        self.url = url
        self.driver = PlaywrightWebDriverSync(headless=False)
        self.session = agentql.start_session(url, web_driver=self.driver)
    
    def stop(self):
        self.session.stop()
