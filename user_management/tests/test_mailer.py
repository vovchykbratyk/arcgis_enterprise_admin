import unittest

from mailer import build_warning_email, build_demoted_email


class TestMailer(unittest.TestCase):
    def test_warning_email_shape(self):
        msg = build_warning_email(
            to_addr="user@example.com",
            username="jdoe",
            full_name="Jane Doe",
            days_inactive=20,
            days_until_demotion=15,
            portal_home_url="https://portal.example.com/portal/home/",
            from_addr="gis-admin@example.com",
            reply_to="helpdesk@example.com",
        )
        self.assertEqual(msg["To"], "user@example.com")
        self.assertEqual(msg["From"], "gis-admin@example.com")
        self.assertEqual(msg["Reply-To"], "helpdesk@example.com")
        self.assertIn("inactivity notice", msg["Subject"])
        body = msg.get_content()
        self.assertIn("20 day(s)", body)
        self.assertIn("https://portal.example.com/portal/home/", body)

    def test_demoted_email_shape(self):
        msg = build_demoted_email(
            to_addr="user@example.com",
            username="jdoe",
            full_name=None,
            days_inactive=40,
            portal_home_url="https://portal.example.com/portal/home/",
            from_addr="gis-admin@example.com",
            reply_to=None,
        )
        self.assertEqual(msg["To"], "user@example.com")
        self.assertEqual(msg["From"], "gis-admin@example.com")
        self.assertIsNone(msg.get("Reply-To"))
        self.assertIn("reduced to Viewer", msg["Subject"])
        body = msg.get_content()
        self.assertIn("40 day(s)", body)