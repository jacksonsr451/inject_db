import logging
import subprocess


def main():
    try:
        subprocess.run(
            ['streamlit', 'run', 'inject_db/app.py', '--server.runOnSave=true']
        )
    except KeyboardInterrupt:
        logging.info('Processo interrompido pelo usuário.')


if __name__ == '__main__':
    main()
