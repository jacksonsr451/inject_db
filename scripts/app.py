import streamlit as st

# Seleção do tipo de arquivo
st.title("Escolha o tipo de arquivo para processar")
file_type = st.selectbox("Selecione o tipo de arquivo:", ["CSV", "XLSX", "JSON", "ODS"])

# Execução com base na seleção
if file_type == "CSV":
    import modules.csv_process as csv_module
    csv_module.run()
elif file_type == "XLSX":
    import modules.xlsx_process as xlsx_module
    xlsx_module.run()
elif file_type == "JSON":
    import modules.json_process as json_module
    json_module.run()
elif file_type == "ODS":
    import modules.ods_process as ods_module
    ods_module.run()
