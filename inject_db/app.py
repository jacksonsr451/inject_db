import streamlit as st

# Seleção do tipo de arquivo
st.title('Escolha o tipo de arquivo para processar')
file_type = st.selectbox(
    'Selecione o tipo de arquivo:', ['CSV', 'XLSX', 'JSON', 'ODS', 'POSTGRES']
)

# Execução com base na seleção
if file_type == 'CSV':
    import inject_db.modules.csv_process as csv_module

    csv_module.run()
elif file_type == 'XLSX':
    import inject_db.modules.xlsx_process as xlsx_module

    xlsx_module.run()
elif file_type == 'JSON':
    import inject_db.modules.json_process as json_module

    json_module.run()
elif file_type == 'ODS':
    import inject_db.modules.ods_process as ods_module

    ods_module.run()
elif file_type == 'POSTGRES':
    import inject_db.modules.postgres_process as postgres_module

    postgres_module.run()
