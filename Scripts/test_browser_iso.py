"""Real Chromium isolation checks against the isolated local live proxy."""
import unittest
import test_live_proxy as live

class BrowserIsolationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            raise unittest.SkipTest("Install Playwright and Chromium to run browser checks")
        cls.playwright = sync_playwright().start()
        cls.addClassCleanup(cls.playwright.stop)
        from pathlib import Path
        if not Path(cls.playwright.chromium.executable_path).exists():
            raise unittest.SkipTest("Playwright Chromium is not installed")
        cls.browser = cls.playwright.chromium.launch(headless=True)
        cls.addClassCleanup(cls.browser.close)
        cls.fixture = type("BrowserProxyFixture", (live.LiveProxyTests,),
                           {"CONFIG_OVERRIDES": {"CROSS_ORIGIN_ISOLATION": True}})
        cls.addClassCleanup(cls.fixture.doClassCleanups)
        cls.fixture.setUpClass()

    def test_live_page_is_isolated_and_javascript_runs(self):
        page = self.browser.new_page()
        self.addCleanup(page.close)
        failures = []
        page.on("pageerror", lambda error: failures.append(str(error)))
        page.goto(self.fixture.base, wait_until="networkidle", timeout=20000)
        self.assertEqual(page.title(), "Site2Local live test")
        self.assertTrue(page.evaluate("window.liveFixture === true"))
        self.assertTrue(page.evaluate("window.crossOriginIsolated"))
        self.assertEqual(page.evaluate("typeof SharedArrayBuffer"), "function")
        self.assertEqual(failures, [])

if __name__ == "__main__":
    unittest.main()
