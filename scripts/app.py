import streamlit as st

# Seleção do tipo de arquivo
st.title("Escolha o tipo de arquivo para processar")
file_type = st.selectbox("Selecione o tipo de arquivo:", ["CSV", "XLSX"])

# Execução com base na seleção
if file_type == "CSV":
    import modules.csv_process as csv_module
    csv_module.run()
elif file_type == "XLSX":
    import modules.xlsx_process as xlsx_module
    xlsx_module.run()
