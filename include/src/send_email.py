from airflow.hooks.base_hook import BaseHook
from email.message import EmailMessage
from .postgres_loader import Postgres
from airflow.models import Variable
from io import BytesIO
import smtplib
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class Email:
    def __init__(self, conn_id):
        self.conn_id = conn_id
        self.postgres = Postgres(conn_id='postgres_connection')
        self.smtp_connection = self.get_smtp_connection()
        
    def get_smtp_connection(self):
        conn = BaseHook.get_connection(self.conn_id)
        return {
            "host": conn.host,
            "port": conn.port,
            "user": conn.login,
            "password": conn.password,
            "tls": conn.extra_dejson.get('tls', True)
        }

    def send_email_with_attachment(self, excel_data: bytes, recipient_email: str, subject: str = "Daily Report", body: str = "Hi @all, Please find the attached report."):
        smtp_conn = self.smtp_connection
        msg = EmailMessage()
        msg['From'] = smtp_conn['user']
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.set_content(body)

        # Attach the Excel file
        msg.add_attachment(excel_data, maintype='application', subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet', filename="report.xlsx")

        logger.info(f"Sending email to {recipient_email} with subject: {subject}")

        with smtplib.SMTP(smtp_conn['host'], smtp_conn['port']) as server:
            if smtp_conn['tls']:
                server.starttls()
            server.login(smtp_conn['user'], smtp_conn['password'])
            server.send_message(msg)

    def extract_and_email_excel(self, query: str, email_list_path: str):
        df = self.postgres.fetch_to_dataframe(query)

        if df is None or df.empty:
            raise ValueError("DataFrame is empty or None")

        excel_buffer = BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)

        email_df = pd.read_excel(email_list_path)
        email_list = email_df['Email'].tolist()

        logger.info(f"Loaded {len(email_list)} email addresses from {email_list_path}: {email_list}")

        for recipient_email in email_list:
            self.send_email_with_attachment(excel_buffer.getvalue(), recipient_email)
