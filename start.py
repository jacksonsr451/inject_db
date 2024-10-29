import subprocess
import logging

def main():
    try:
        subprocess.run(["streamlit", "run", "scripts/app.py", "--server.runOnSave=true"])
    except KeyboardInterrupt:
        logging.info("Processo interrompido pelo usuário.")


if __name__ == "__main__":
    main()