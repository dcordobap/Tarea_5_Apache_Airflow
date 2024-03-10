import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from pickle import dump
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from sklearn.metrics import f1_score




def preprocess_train_save():
    pd.set_option('display.max_rows', None)  # Ninguna restricción en el número de filas
    pd.set_option('display.max_columns', None)  # Ninguna restricción en el número de columnas

    hook = PostgresHook(postgres_conn_id='postgres_default')
    df = hook.get_pandas_df(sql="SELECT * FROM penguins;")
    df.fillna({'bodymassg': 1, 'flipperlength': 1}, inplace=True)
    # Preprocesamiento de los datos
    data_cleaned = df.loc[:,['species', 'island', "clutchcompletion", 'culmenlength',
                             'culmendepth', 'flipperlength', 'bodymassg', 'sex',
                             'delta15n', 'delta13c']]
                             
    data_cleaned["Body_Mass_ln"] = np.log(data_cleaned['bodymassg']).replace([np.inf, -np.inf, np.nan], 0)
    data_cleaned["Flipper_Length_ln"] = np.log(data_cleaned['flipperlength']).replace([np.inf, -np.inf, np.nan], 0)

    data_cleaned= data_cleaned.replace([np.inf, -np.inf], np.nan).dropna()
    data_model = data_cleaned.loc[:,['species', 'island', "clutchcompletion", 'culmenlength',
                                      'culmendepth', 'sex',
                                      'delta15n', 'delta13c', 'Body_Mass_ln', 
                                      'Flipper_Length_ln']]
                           
    col_one_hot = ['island', "clutchcompletion", 'sex']
    data_encoded = pd.get_dummies(data_model, columns=col_one_hot)
    
    label_encoder = LabelEncoder()
    data_encoded['Species_encode'] = label_encoder.fit_transform(data_encoded['species'])
    
    y = data_encoded['Species_encode']
    X = data_encoded.drop(['species', "Species_encode"], axis=1)
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    model = RandomForestClassifier(n_estimators=100, max_depth=3, random_state=42)
    model.fit(X_train, y_train)
    
    y_pred = model.predict(X_test)
    categories = label_encoder.inverse_transform(y_test.unique())
    y_test_categories = label_encoder.inverse_transform(y_test)
    y_pred_categories = label_encoder.inverse_transform(y_pred)
    
    
    cm = confusion_matrix(y_test_categories, y_pred_categories, labels=categories)

    # Crear un DataFrame de pandas con la matriz de confusión para mejor visualización
    cm_df = pd.DataFrame(cm, index=categories, columns=categories)
    accuracy = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred, average='weighted')

    print(f'Accuracy: {accuracy}')
    print(classification_report(y_test, y_pred))
    print(confusion_matrix(y_test, y_pred))
    
    model_vars = ', '.join(X.columns)
    content = f"Variables del modelo X: {model_vars}\n"
    content += f"Accuracy: {accuracy}\n"
    content += f"F1: {f1}\n"
    content += "Matriz de confusión del conjunto de test:\n"
    content += f"{cm_df}\n"
    cross_table = pd.crosstab(data_encoded['species'], data_encoded['Species_encode'])
    print(cross_table)
    
    # Guardar los resultados y el modelo
    cross_table.to_csv('/opt/airflow/results/cross_table.csv', index=False)

    dump(model, open('/opt/airflow/results/model_xgboost.pkl', 'wb'))

    with open('/opt/airflow/results/Model_results.txt', 'w') as file:
        file.write(content);



# Función que será ejecutada por la tarea
def print_hello():
    with open('/opt/airflow/results/Model_results.txt', 'w') as file:
        file.write()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'preprocess_train_save_model',
    default_args=default_args,
    description='Preprocess data, train model, and save results',
    schedule_interval=None,
    catchup=False,
)

task = PythonOperator(
    task_id='preprocess_train_save',
    python_callable=preprocess_train_save,
    dag=dag,
)