from flask import Flask, jsonify, render_template
import psycopg2
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import io
import base64
from threading import Thread
import schedule
import subprocess
import time
import datetime
import requests
from datetime import timezone
from datetime import timedelta
import pytz

app = Flask(__name__)

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host='52.201.84.249',
            database='airflow',
            user='airflow',
            password='airflow',
            port=5432
        )
        print("Connected to the database")
        return conn
    except Exception as e:
        print(f"Error: {e}")
        return None
    
conn = get_db_connection()

################################################################################################
def get_dag_run_metrics():
    if conn:
        try:
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(DISTINCT dag_id) FROM dag_run;")
            total_dags = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM dag_run;")
            total_runs = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM dag_run WHERE state = 'success';")
            successful_runs = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM dag_run WHERE state = 'failed';")
            failed_runs = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM dag_run WHERE state = 'running';")
            running_runs = cursor.fetchone()[0]

            cursor.execute("SELECT 1;") 
            metadatabase_status = 'Healthy' if cursor.fetchone() else 'Unhealthy'

            current_time = datetime.datetime.now(timezone.utc)
            
            cursor.execute("SELECT MAX(latest_heartbeat) FROM job WHERE job_type = 'TriggererJob';")
            triggerer_heartbeat = cursor.fetchone()[0]  or 'Unavailable'
            
            if(triggerer_heartbeat != 'Unavailable') :
                time_difference = current_time - triggerer_heartbeat
                triggerer_heartbeat = str(int(time_difference.total_seconds())) + " seconds ago"
                

            cursor.execute("SELECT MAX(latest_heartbeat) FROM job WHERE job_type = 'SchedulerJob';")
            scheduler_heartbeat = cursor.fetchone()[0] or 'Unavailable'
            
            if(scheduler_heartbeat != 'Unavailable') :
                time_difference = current_time - scheduler_heartbeat
                scheduler_heartbeat = str(int(time_difference.total_seconds())) + " seconds ago"

            cursor.close()

            return {
                'total_dags': total_dags,
                'total_runs': total_runs,
                'successful_runs': successful_runs,
                'failed_runs': failed_runs,
                'running_runs': running_runs,
                'metadatabase_status': metadatabase_status,
                'triggerer_heartbeat': triggerer_heartbeat,
                'scheduler_heartbeat': scheduler_heartbeat,
            }

        except Exception as e:
            print(f"Error while fetching data: {e}")
            return None
    return None

@app.route('/', methods=['GET'])
def homepage():
    metrics = get_dag_run_metrics()
    if metrics:
        return render_template('homepage.html', **metrics)
    else:
        return "Error fetching DAG run metrics", 500
################################################################################################

@app.route('/tableview', methods=['GET'])
def tableview():
    return render_template('tableview.html')

@app.route('/get_dag_data', methods=['GET'])
def get_dag_data():
    if conn is None:
        return jsonify({'error': 'Database connection failed'}), 500

    cur = conn.cursor()
    cur.execute('''
        SELECT 
            d.owners, 
            dr.dag_id, 
            d.schedule_interval,  
            dr.run_id, 
            dr.run_type, 
            dr.start_date, 
            dr.end_date, 
            dr.state, 
            ROUND(SUM(CAST(ti.duration AS DECIMAL)), 2) as total_duration
        FROM 
            dag d
        JOIN 
            dag_run dr ON d.dag_id = dr.dag_id  
        JOIN 
            task_instance ti ON dr.run_id = ti.run_id 
        GROUP BY 
            d.owners, dr.dag_id, d.schedule_interval, dr.run_id, dr.run_type, dr.start_date, dr.end_date, dr.state
        ORDER BY 
            dr.end_date DESC;
    ''')
    
    rows = cur.fetchall()
    cur.close()

    dag_run_list = []
    for row in rows:
        dag_run_list.append({
            'owner': row[0],
            'dag_id': row[1],
            'schedule_interval': row[2],
            'run_id': row[3], 
            'run_type':row[4], 
            'start_date':row[5].isoformat() if row[5] else 'N/A',
            'end_date': row[6].isoformat() if row[6] else 'N/A',
            'state':row[7],
            'duration':row[8]
        })
    return jsonify(dag_run_list)

#################################################################################################################

def get_dags_data():
    if conn is None:
        return None

    query = '''
        SELECT id, dag_id, queued_at, execution_date, start_date, end_date, state, run_id, creating_job_id, external_trigger, run_type, data_interval_start, data_interval_end, last_scheduling_decision, dag_hash, log_template_id 
        FROM dag_run
        ORDER BY end_date DESC;
    '''
    df = pd.read_sql(query, conn)
    return df

def generate_state_pie_chart(df):
    state_counts = df['state'].value_counts()

    plt.figure(figsize=(6, 6))
    wedges, texts, autotexts = plt.pie(
        state_counts,
        startangle=90,
        colors=plt.cm.Paired.colors,
        autopct='%1.1f%%',
        textprops={'color': "w"},
    )
    
    plt.legend(wedges, state_counts.index, title="States", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))
    plt.title('DAG States Distribution')
    
    img = io.BytesIO()
    plt.savefig(img, format='png', bbox_inches="tight")
    img.seek(0)
    plot_url = base64.b64encode(img.getvalue()).decode('utf8')
    plt.close() 

    return plot_url

def generate_dag_run_graph(df):
    df['execution_date'] = pd.to_datetime(df['execution_date']).dt.date

    last_10_days = pd.to_datetime('today').date() - pd.DateOffset(days=10)
    recent_df = df[df['execution_date'] >= last_10_days.date()]

    dag_counts_by_date = recent_df.groupby('execution_date').size()

    plt.figure(figsize=(10, 6))
    dag_counts_by_date.plot(kind='bar', color='skyblue')
    plt.title('Number of DAGs Scheduled in the Last 10 Days')
    plt.xlabel('Execution Date')
    plt.ylabel('Number of DAGs')
    
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    
    plot_url = base64.b64encode(img.getvalue()).decode('utf8')
    plt.close()  

    return plot_url

def generate_last_100_dag_execution_times(df):
    # df['execution_date'] = pd.to_datetime(df['execution_date'])
    
    # last_100_dags = df.sort_values(by='execution_date', ascending=False).head(100)

    # last_100_dags['execution_time'] = (last_100_dags['end_date'] - last_100_dags['start_date']).dt.total_seconds() / 60

    # plt.figure(figsize=(8, 6))
    # plt.barh(last_100_dags['dag_id'], last_100_dags['execution_time'], color='lightgreen')
    # plt.title('DAG Name vs Execution Time of Last 100 DAG Runs')
    # plt.xlabel('Execution Time (minutes)')
    # plt.ylabel('DAG Name')
    # plt.tight_layout()

    # img = io.BytesIO()
    # plt.savefig(img, format='png')
    # img.seek(0)
    # plot_url = base64.b64encode(img.getvalue()).decode('utf8')
    # plt.close()
    # return plot_url
    
    
    # Ensure end_date and start_date are datetime objects
    df['end_date'] = pd.to_datetime(df['end_date'])
    df['start_date'] = pd.to_datetime(df['start_date'])
    
    # Get the last 100 DAGs sorted by end_date
    last_100_dags = df.sort_values(by='end_date', ascending=False).head(100)
    
    # Calculate the execution duration in minutes
    last_100_dags['execution_duration'] = (last_100_dags['end_date'] - last_100_dags['start_date']).dt.total_seconds() / 60

    # Sort by end_date to plot in chronological order
    last_100_dags = last_100_dags.sort_values(by='end_date')

    # Create a uniform x-axis for DAG numbers
    dag_numbers = range(1, len(last_100_dags) + 1)

    plt.figure(figsize=(12, 6))
    plt.plot(dag_numbers, last_100_dags['execution_duration'], marker='o', linestyle='-', color='blue')
    plt.title('Execution Duration of the Last 100 DAG Runs')
    plt.xlabel('DAG Number')
    plt.ylabel('Execution Duration (minutes)')
    # plt.xticks(dag_numbers)  # Set x-ticks to correspond to DAG numbers
    plt.grid()
    
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    
    plot_url = base64.b64encode(img.getvalue()).decode('utf8')
    plt.close()
    
    return plot_url

@app.route('/analytics', methods=['GET'])
def analytics():
    df = get_dags_data()
    if df is None or df.empty:
        return render_template('analytics.html', error_message="No data available for analytics")
    state_pie = generate_state_pie_chart(df)
    dag_run_graph = generate_dag_run_graph(df)
    dag_execution_times_chart = generate_last_100_dag_execution_times(df)

    return render_template('analytics.html', state_pie=state_pie, dag_run_graph=dag_run_graph, dag_execution_times_chart=dag_execution_times_chart)


#####################################################################################################################

@app.route('/alerts', methods=['GET'])
def alerts():
    return render_template('alerts.html')

alerts = []

@app.route('/get_alerts', methods=['GET'])
def get_alerts():
    return jsonify(alerts)

def send_email():
    try:
        result = subprocess.run(['python', 'C:\\Users\\Jayakrishna\\OneDrive\\Desktop\\DASHBOARD\\send_email.py'], check=True, capture_output=True)
        alerts.append(f"Email sent successfully at {datetime.datetime.now()}.")
    except subprocess.CalledProcessError as e:
        alerts.append(f"Error sending email: {e.stderr.decode()}")
        

        
def check_current_dag_status():
    if conn:
        try:
            cursor = conn.cursor()
            current_time_utc = datetime.datetime.now(timezone.utc)
            cursor.execute("""
                SELECT dag_id, state
                FROM dag_run 
                WHERE end_date >= %s AND end_date <= %s and state = 'failed'
                LIMIT 1;
            """, (current_time_utc - datetime.timedelta(seconds=1), current_time_utc))
            
            failed_dags = cursor.fetchall()
            
            if failed_dags:
                for dag_run in failed_dags:
                    cursor.execute("""
                        SELECT 
                    """)
                    alerts.append(f"DAG_ID: {dag_run[0]}, State: {dag_run[1]}")
                    
            cursor.close()
        except Exception as e:
            print(f"Error while fetching failed DAGs: {e}")
            return []

def run_scheduler():
    schedule.every().day.at("00:04").do(send_email) 
    while True:
        check_current_dag_status()
        schedule.run_pending()
        time.sleep(1)

######################################################################################################

def get_failed_dags():
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT dr.dag_id, dr.end_date, ROUND(SUM(CAST(ti.duration AS DECIMAL)), 2), dr.state
                FROM dag_run dr
                JOIN task_instance ti ON dr.run_id = ti.run_id
                WHERE dr.state != 'success'
                GROUP BY dr.dag_id, dr.end_date, dr.state
                ORDER BY dr.end_date DESC;
            """)
            failed_dags = cursor.fetchall()
            cursor.close()
            return failed_dags
        except Exception as e:
            print(f"Error while fetching failed DAGs: {e}")
            return []
    return []

@app.route('/reschedule', methods=['GET'])
def schedulingTechniques():
    failed_dags = get_failed_dags() 
    return render_template('reschedule.html', failed_dags=failed_dags)
 
@app.route('/reschedule_dags/<strategy>', methods=['POST'])
def reschedule_dags(strategy):
    try:
        if conn is None:
            return jsonify({'message': 'Database connection failed'}), 500
        
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT dr.run_id, dr.dag_id, dr.end_date, SUM(ti.duration) as duration 
                FROM dag_run dr
                JOIN task_instance ti ON dr.run_id = ti.run_id
                WHERE dr.state != 'success'
                GROUP BY dr.run_id, dr.dag_id, dr.end_date;
            """)
            # as duration
        except Exception as e:
            return jsonify(f" {str(e)}")
        
        failed_dags = cur.fetchall()
        
        if not failed_dags:
            return jsonify({'message': 'No failed DAGs to reschedule'}), 200
        
        if(strategy == 'lifo'):
            print('sorted by lifo')
            failed_dags = sorted(failed_dags, key=lambda x: str(x[2]), reverse=True)
        elif(strategy == 'fifo'):
            print('sorted by fifo')
            failed_dags = sorted(failed_dags, key=lambda x: str(x[2]))
        elif(strategy == 'duration_asc'):
            print('sorted by duration_asc')
            failed_dags = sorted(failed_dags, key=lambda x: x[3])
        elif(strategy == 'duration_desc'):
            print('sorted by duration_desc')
            failed_dags = sorted(failed_dags, key=lambda x: x[3], reverse=True)
        
        for run_id, dag_id, end_date, duration in failed_dags:
            # execution_date_str = str(end_date) 
            # print(execution_date_str)
            
            # cur.execute(f"DELETE FROM dag_run WHERE run_id='{run_id}';")
            # cur.execute(f"DELETE FROM task_instance WHERE run_id='{run_id}' and state != 'success';")
            # # conn.commit() 
            # cur.execute(f"""
            #     INSERT INTO task_instance (dag_id, task_id, execution_date, run_id, state)
            #     SELECT dag_id, task_id, execution_date, run_id, 'scheduled'
            #     FROM task_instance
            #     WHERE dag_id = '{dag_id}' AND run_id = '{run_id}' AND state IS NULL;
            # """)
            # cur.execute(f"""
            #     delete from dag_run
            #     WHERE run_id = '{run_id}' 
            #     AND dag_id = '{dag_id}' 
            #     AND state = 'success';
            # """)
            
            cur.execute(f"""
                UPDATE dag_run
                SET state = 'queued'
                WHERE run_id = '{run_id}' 
                AND dag_id = '{dag_id}' 
                AND state != 'success';
            """)
            
            cur.execute(f"""
                UPDATE task_instance
                SET state = NULL
                WHERE run_id = '{run_id}' 
                AND dag_id = '{dag_id}' 
                AND state != 'success';
            """)
            # conn.commit()
            conn.commit() 
            # trigger_url = f'http://localhost:8081/api/v1/dags/{dag_id}/dagRuns'
            # data = {'execution_date': execution_date_str} 
            # response = requests.post(trigger_url, json=data, auth=('airflow', 'airflow'))
            
            # if response.status_code != 200:
                # print("could  not trigger")
                
    except Exception as e:
        return jsonify({'message': f'An error occurred: {str(e)}'}), 500
   
    alerts.append(f"Failed DAGs are rescheduled by strategy : {strategy}")
    return jsonify({'message': f'Successfully rescheduled {len(failed_dags)} DAGs using {strategy} strategy'}), 200

##############################################################################################################

if __name__ == '__main__':
    scheduler_thread = Thread(target=run_scheduler)
    scheduler_thread.daemon = True  
    scheduler_thread.start()
    app.run(debug=True)