import smtplib
from datetime import datetime, timedelta
import psycopg2
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

sender_email = "airflow.jk.14@gmail.com"
sender_password = "mcnz rvzr fkhb bozs"  

db_config = {
    'host': '52.201.84.249',
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
    'port': 5432
}

def get_dag_run_statuses():
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        query = '''
            SELECT id, dag_id, state 
            FROM dag_run 
            WHERE end_date >= NOW() - INTERVAL '24 HOURS'
            ORDER BY state; 
        '''
        cursor.execute(query)
        dag_runs = cursor.fetchall()
        cursor.close()
        conn.close()
        return dag_runs
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return []
    
def format_dag_run_statuses(dag_runs):
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Initialize the HTML structure for the table
    body = f"""
    <html>
    <head>
        <style>
            table {{
                width: 100%;
                border-collapse: collapse;
            }}
            table, th, td {{
                border: 1px solid black;
            }}
            th, td {{
                padding: 10px;
                text-align: left;
            }}
            th {{
                background-color: #000000;
                color: white;
            }}
        </style>
    </head>
    <body>
        <p>DAG Run Statuses as of {current_time}:</p>
        <table>
            <tr>
                <th>DAG Run ID</th>
                <th>DAG ID</th>
                <th>State</th>
            </tr>
    """

    # Add rows for each DAG run with conditional formatting based on state
    for run in dag_runs:
        state = run[2]
        
        # Determine row color based on state
        if state == "success":
            row_color = "background-color: darkgreen; color: white;"
        elif state == "failed":
            row_color = "background-color: red; color: white;"
        elif state == "up_for_retry":
            row_color = "background-color: yellow; color: black;"
        elif state == "running":
            row_color = "background-color: lightgreen; color: black;"
        else:
            row_color = ""

        # Add the table row with the appropriate color
        body += f"""
            <tr style="{row_color}">
                <td>{run[0]}</td>
                <td>{run[1]}</td> 
                <td>{state}</td>
            </tr>
        """

    # Close the table and the HTML structure
    body += """
        </table>
        <p>End of Report</p>
    </body>
    </html>
    """

    return body



def send_email(receiver_email, subject, body):
    try:
        # Create a MIMEMultipart message
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = sender_email
        msg["To"] = receiver_email

        # Create the HTML version of the message
        html_part = MIMEText(body, "html")  # Specify HTML content here
        msg.attach(html_part)

        # Connect to the SMTP server and send the email
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
            print(f"Email sent to {receiver_email}")
    except Exception as e:
        print(f"Failed to send email to {receiver_email}. Error: {e}")

dag_runs = get_dag_run_statuses()

if dag_runs:
    subject = "DAG Run Statuses"
    body = format_dag_run_statuses(dag_runs)
    send_email("kk8334787@gmail.com", subject, body)
else:
    # print("No DAG run statuses found.")
    subject = "DAG Run Statuses"
    body = "All Scheduled DAGs have run successfully"
    send_email("kk8334787@gmail.com", subject, body)
    
print(body)